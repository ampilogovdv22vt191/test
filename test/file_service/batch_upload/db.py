from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, JSON, String, Text, create_engine, delete, update
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker


# Активные задачи видны в ленте независимо от возраста. Восстановление после
# сбоя касается только задач, которые уже взял воркер.
ACTIVE = ("queued", "validating", "uploading", "cancelling")
IN_PROGRESS = ACTIVE[1:]
READABLE = ("uploaded", "exists")


def now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Job(Base):
    """Запись об одном ZIP со статусом, временем обработки и счётчиками."""

    __tablename__ = "batch_jobs"
    __table_args__ = (Index("batch_jobs_queue", "status", "created_at"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    archive_name: Mapped[str] = mapped_column(Text)
    # Имя адаптера фиксируется при приёме ZIP и используется при скачивании.
    backend: Mapped[str] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(24), default="queued")
    total: Mapped[int | None]
    processed: Mapped[int] = mapped_column(default=0)
    uploaded: Mapped[int] = mapped_column(default=0)
    exists: Mapped[int] = mapped_column(default=0)
    errors: Mapped[int] = mapped_column(default=0)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    files: Mapped[list["JobFile"]] = relationship(order_by="JobFile.position")


class JobFile(Base):
    """Результат сопоставления записи из data.json или файла без метаданных."""

    __tablename__ = "batch_files"
    __table_args__ = (Index("batch_files_job_status_position", "job_id", "status", "position"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    job_id: Mapped[UUID] = mapped_column(ForeignKey("batch_jobs.id", ondelete="CASCADE"))
    position: Mapped[int]
    filename: Mapped[str] = mapped_column(Text)
    # Сейчас путь в ZIP совпадает с ключом для скачивания из хранилища.
    # Для дедупликации по хешу между разными путями нужен отдельный ключ
    # сохранённого объекта. API должен будет читать файл по этому ключу.
    archive_path: Mapped[str | None] = mapped_column(Text)
    size: Mapped[int | None] = mapped_column(BigInteger)
    # Здесь хранится запись из data.json. Долговременный индекс хешей храни
    # отдельно от batch_files, поскольку эти строки удаляются по сроку.
    meta: Mapped[dict | None] = mapped_column(JSON(none_as_null=True))
    status: Mapped[str] = mapped_column(String(24))
    reason: Mapped[str | None] = mapped_column(Text)


class Revision(Base):
    """Единственная строка со счётчиком изменений для SSE."""

    __tablename__ = "batch_revision"

    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[int] = mapped_column(BigInteger)


class Database:
    def __init__(self, url):
        self.engine = create_engine(url, pool_pre_ping=True)
        self.session = sessionmaker(self.engine)

    def init_schema(self) -> None:
        """Создаёт таблицы и начальную ревизию при первом запуске."""

        from sqlalchemy.dialects.postgresql import insert

        with self.engine.begin() as conn:
            Base.metadata.create_all(conn)
            conn.execute(insert(Revision).values(id=1, value=0).on_conflict_do_nothing())

    def delete_expired_jobs(self, retention_hours: int) -> int:
        """Удаляет завершённые задачи и их строки файлов по сроку хранения."""

        cutoff = now() - timedelta(hours=retention_hours)
        with self.session.begin() as session:
            # PostgreSQL сам удалит связанные batch_files через ON DELETE CASCADE.
            result = session.execute(
                delete(Job)
                .where(Job.finished_at <= cutoff, Job.status.not_in(ACTIVE))
                .execution_options(synchronize_session=False)
            )
            if result.rowcount:
                bump_revision(session)
            return result.rowcount


def bump_revision(session) -> None:
    """Меняет ревизию в той же транзакции, что и статус задачи или файла."""

    session.execute(update(Revision).where(Revision.id == 1).values(value=Revision.value + 1))
