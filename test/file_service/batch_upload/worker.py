from dataclasses import dataclass
import fcntl
import logging
from pathlib import Path
import shutil
import signal
from threading import Event, Thread
from uuid import UUID, uuid4
from zipfile import BadZipFile, ZipFile

from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError

from .archive import ArchiveError, Entry, inspect_zip
from .db import IN_PROGRESS, Database, Job, JobFile, bump_revision, now
from .storage import Storage


log = logging.getLogger(__name__)


def remove_spool(root: Path, job_id: UUID) -> None:
    """Удаляет временный ZIP и его каталог после обработки или отмены."""

    try:
        shutil.rmtree(root / str(job_id))
    except FileNotFoundError:
        pass
    except OSError:
        log.exception("Не удалось удалить временный архив %s", job_id)


def finish_job(session, job: Job, status: str, error: str | None = None) -> None:
    """Фиксирует итог задачи и при необходимости закрывает необработанные строки."""

    if status in ("cancelled", "interrupted", "failed"):
        file_status, reason = {
            "cancelled": ("cancelled", "job_cancelled"),
            "interrupted": ("interrupted", "worker_interrupted"),
            "failed": ("error", "job_failed"),
        }[status]
        result = session.execute(
            update(JobFile)
            .where(JobFile.job_id == job.id, JobFile.status.in_(("pending", "processing")))
            .values(status=file_status, reason=reason)
        )
        if status == "failed":
            job.errors += result.rowcount
            job.processed += result.rowcount
    job.status = status
    job.error = error
    job.finished_at = job.updated_at = now()
    bump_revision(session)


@dataclass(frozen=True)
class WorkFile:
    """Данные для обработки файла после закрытия транзакции PostgreSQL."""

    id: UUID
    archive_path: str
    size: int


class Worker:
    """Обрабатывает архивы последовательно и сохраняет прогресс в PostgreSQL."""

    def __init__(self, db: Database, storages: dict[str, Storage], config):
        self.db = db
        self.storages = storages
        self.config = config
        self.spool = Path(config["BATCH_WORK_DIR"])
        self.stop = Event()

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())
        # Блокировка в общем локальном каталоге защищает от запуска второго воркера.
        with (self.spool / ".worker.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.recover_interrupted()
            log.info("Обработчик архивов запущен")
            # Очистка идёт независимо от записи файлов в Samba в том же контейнере.
            Thread(target=self.clean_history, name="batch-retention", daemon=True).start()
            try:
                while not self.stop.is_set():
                    claim = self.claim()
                    if claim is None:
                        self.stop.wait(self.config["BATCH_WORKER_INTERVAL"])
                        continue
                    job_id, backend = claim
                    self.process(job_id, backend)
                    remove_spool(self.spool, job_id)
            finally:
                self.stop.set()

    def clean_history(self) -> None:
        """Проверяет срок хранения при старте воркера и затем раз в минуту."""

        while not self.stop.is_set():
            try:
                expired = self.db.delete_expired_jobs(self.config["BATCH_RETENTION_HOURS"])
                if expired:
                    log.info("Удалено просроченных записей о загрузках: %s", expired)
            except SQLAlchemyError:
                log.exception("Не удалось очистить историю загрузок")
            self.stop.wait(60)

    def recover_interrupted(self) -> None:
        """Закрывает задачи прошлого процесса и сохраняет подтверждённые результаты."""

        with self.db.session.begin() as session:
            jobs = session.scalars(select(Job).where(Job.status.in_(IN_PROGRESS)).with_for_update()).all()
            ids = [job.id for job in jobs]
            for job in jobs:
                finish_job(session, job, "interrupted", "Обработчик был остановлен во время загрузки")
        for job_id in ids:
            remove_spool(self.spool, job_id)

    def claim(self) -> tuple[UUID, str] | None:
        """Берёт самую старую доступную задачу и переводит её на проверку ZIP."""

        with self.db.session.begin() as session:
            job = session.scalar(
                select(Job)
                .where(Job.status == "queued")
                .order_by(Job.created_at, Job.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if job is None:
                return None
            job.status = "validating"
            job.updated_at = now()
            bump_revision(session)
            return job.id, job.backend

    def prepare(self, job_id: UUID, entries: list[Entry]) -> None:
        """Создаёт строки для фронтенда до начала записи файлов в хранилище."""

        with self.db.session.begin() as session:
            job = session.get(Job, job_id, with_for_update=True)
            if job.status == "cancelling":
                return
            errors = 0
            for position, entry in enumerate(entries):
                session.add(
                    JobFile(
                        id=uuid4(),
                        job_id=job_id,
                        position=position,
                        filename=entry.filename,
                        archive_path=entry.member.filename if entry.member else None,
                        size=entry.member.file_size if entry.member else None,
                        meta=entry.meta,
                        status="error" if entry.error else "pending",
                        reason=entry.error,
                    )
                )
                errors += bool(entry.error)
            job.total = len(entries)
            # Ошибки сопоставления уже имеют итоговый статус и входят в прогресс.
            job.processed = job.errors = errors
            job.status = "uploading"
            job.updated_at = now()
            bump_revision(session)

    def take_file(self, job_id: UUID) -> WorkFile | None:
        """Проверяет отмену и выбирает следующую строку для обработки."""

        with self.db.session.begin() as session:
            job = session.get(Job, job_id, with_for_update=True)
            if job.status == "cancelling":
                finish_job(session, job, "cancelled")
                return None
            if job.processed == job.total:
                finish_job(session, job, "done_with_errors" if job.errors else "done")
                return None
            if self.stop.is_set():
                finish_job(session, job, "interrupted", "Обработчик был остановлен во время загрузки")
                return None
            item = session.scalars(
                select(JobFile)
                .where(JobFile.job_id == job_id, JobFile.status == "pending")
                .order_by(JobFile.position)
                .limit(1)
            ).one()
            item.status = "processing"
            job.updated_at = now()
            bump_revision(session)
            # Если для проверки содержимого нужны метаданные, добавь поле meta
            # в WorkFile и передай сюда item.meta, пока сессия открыта.
            return WorkFile(item.id, item.archive_path, item.size)

    def record_file(self, job_id: UUID, file_id: UUID, status: str, reason: str | None) -> None:
        """Сохраняет результат файла, счётчики задачи и ревизию одной транзакцией."""

        # Даже пропущенный файл проходит здесь один раз. Так фронт видит
        # итог по файлу, счётчики сходятся и задача доходит до завершения.
        # Если вводишь новый статус пропуска, добавь его обработку в счётчики
        # и согласуй с фронтом, как показать результат.
        with self.db.session.begin() as session:
            job = session.get(Job, job_id, with_for_update=True)
            item = session.get(JobFile, file_id)
            item.status, item.reason = status, reason
            job.processed += 1
            if status == "uploaded":
                job.uploaded += 1
            elif status == "exists":
                job.exists += 1
            else:
                job.errors += 1
            job.updated_at = now()
            bump_revision(session)

    def process(self, job_id: UUID, backend: str) -> None:
        """Обрабатывает один ZIP и записывает итог по каждому взятому файлу."""

        try:
            storage = self.storages.get(backend)
            if storage is None:
                raise ArchiveError("Хранилище этой загрузки недоступно в конфигурации")
            with ZipFile(self.spool / str(job_id) / "archive.zip") as archive:
                entries = inspect_zip(
                    archive,
                    max_members=self.config["BATCH_MAX_MEMBERS"],
                    max_unpacked=self.config["BATCH_MAX_UNPACKED_BYTES"],
                    max_manifest=self.config["BATCH_MAX_MANIFEST_BYTES"],
                )
                self.prepare(job_id, entries)
                while (item := self.take_file(job_id)) is not None:
                    reason = None
                    try:
                        # Здесь транзакция PostgreSQL уже закрыта. Сюда можно
                        # добавить проверки содержимого перед put_if_absent.
                        # Например, посчитать SHA-256 распакованных байтов
                        # порциями и сравнить с хешем из метаданных.
                        with archive.open(item.archive_path) as source:
                            # Чтение для хеша сдвигает позицию в source. Если
                            # проверка прочитала поток, перед записью открой
                            # этот же файл через archive.open() ещё раз.
                            status = storage.put_if_absent(item.archive_path, source, size=item.size)
                    except (BadZipFile, NotImplementedError):
                        status, reason = "error", "invalid_zip_member"
                    except Exception:
                        log.exception("Ошибка записи файла %s задачи %s", item.id, job_id)
                        status, reason = "error", "storage_error"
                    # Если твоя проверка отклоняет файл, выставь status="error"
                    # и свою reason вместо записи в хранилище. Дойди до
                    # record_file, чтобы файл получил итоговый статус.
                    self.record_file(job_id, item.id, status, reason)
        except ArchiveError as exc:
            self.fail(job_id, str(exc))
        except (BadZipFile, NotImplementedError):
            self.fail(job_id, "Повреждённый ZIP или неподдерживаемый метод сжатия")
        except OSError:
            log.exception("Не удалось прочитать временный архив %s", job_id)
            self.fail(job_id, "Не удалось прочитать временный архив")

    def fail(self, job_id: UUID, error: str) -> None:
        with self.db.session.begin() as session:
            job = session.get(Job, job_id, with_for_update=True)
            if job.status == "cancelling":
                finish_job(session, job, "cancelled")
            else:
                finish_job(session, job, "failed", error)
