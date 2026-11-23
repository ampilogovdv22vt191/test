from collections.abc import Callable
from dataclasses import dataclass, field
import fcntl
from io import BytesIO
import logging
from pathlib import Path
import shutil
from threading import Event, Thread
from typing import TextIO
from uuid import UUID, uuid4
from zipfile import BadZipFile, ZipFile

from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError

from .archive import ArchiveError, Entry, inspect_zip
from .db import IN_PROGRESS, Database, Job, JobFile, bump_revision, now
from .storage import CHUNK_SIZE, SaveFile


log = logging.getLogger(__name__)


def remove_spool(root: Path, job_id: UUID) -> None:
    """Удаляет каталог BATCH_WORK_DIR/<job_id> вместе с временным ZIP.

    Воркер вызывает функцию после завершения задачи. API вызывает её
    после отмены задачи, которую ещё не взяли в работу. Отсутствующий
    каталог считается уже удалённым, а сбой удаления попадает в журнал.
    """

    try:
        shutil.rmtree(root / str(job_id))
    except FileNotFoundError:
        pass
    except OSError:
        log.exception("Не удалось удалить временный архив %s", job_id)


def finish_job(session, job: Job, status: str, error: str | None = None) -> None:
    """Ставит задаче итоговый статус в уже открытой транзакции.

    При отмене, прерывании или общей ошибке закрывает строки файлов,
    которые оставались в pending или processing. Общая ошибка добавляет
    эти строки к счётчикам ошибок и обработанных файлов. Функция сохраняет
    время завершения и меняет ревизию SSE. Транзакцию фиксирует вызывающий код.
    """

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
    # Запись из data.json для этого файла. Доступна и после закрытия сессии БД.
    meta: dict = field(default_factory=dict)


AfterSave = Callable[[WorkFile, object], None]


class Worker:
    """Обрабатывает архивы последовательно и сохраняет прогресс в PostgreSQL."""

    def __init__(
        self,
        db: Database,
        save_file: SaveFile,
        config,
        *,
        file_exists_error: type[Exception] = FileExistsError,
        after_save: AfterSave | None = None,
    ):
        """Сохраняет зависимости воркера и создаёт общий сигнал остановки.

        save_file принимает BytesIO, а file_exists_error указывает исключение
        библиотеки для уже существующего файла. После успешной записи
        after_save получает WorkFile и результат save_file. Настройки config
        задают каталог ZIP и лимиты. Подготовка PostgreSQL выполняется в start().
        """

        self.db = db
        self.save_file = save_file
        self.file_exists_error = file_exists_error
        self.after_save = after_save
        self.config = config
        self.spool = Path(config["BATCH_WORK_DIR"])
        self.stop = Event()
        self.thread: Thread | None = None

    def start(self) -> None:
        """Подготавливает PostgreSQL и запускает поток обработки очереди.

        Сначала блокирует каталог задач от второго обработчика, создаёт
        недостающие таблицы и восстанавливает состояние прерванных задач.
        Затем передаёт открытую блокировку потоку _run(). При ошибке
        подготовки освобождает её и передаёт исключение вызывающему коду.
        """

        lock = (self.spool / ".worker.lock").open("a")
        try:
            # Второй процесс с тем же томом не должен забирать те же задания.
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.db.init_schema()
            self.recover_interrupted()
            self.thread = Thread(target=self._run, args=(lock,), name="batch-worker", daemon=True)
            self.thread.start()
        except BaseException:
            lock.close()
            raise
        log.info("Таблицы готовы, фоновый обработчик архивов запущен")

    def close(self, timeout: float = 10) -> None:
        """Запрашивает остановку и ждёт завершения потока до timeout секунд.

        Воркер проверяет сигнал перед следующим файлом и при ожидании
        очереди. Уже начатой записи даётся время завершиться и сохранить
        результат. После истечения timeout вызывающий код может закрыть процесс.
        """

        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout)

    def _run(self, lock: TextIO) -> None:
        """Обрабатывает очередь по одной задаче, пока удерживается блокировка.

        После каждой задачи удаляет её временный ZIP. При пустой очереди
        ждёт BATCH_WORKER_INTERVAL секунд. После сбоя цикла при следующей
        попытке восстанавливает прерванную задачу, затем снова читает очередь.
        Отдельный поток clean_history() запускается вместе с этим циклом.
        """

        with lock:
            # Очистка идёт независимо от записи файлов в Samba в том же контейнере.
            Thread(target=self.clean_history, name="batch-retention", daemon=True).start()
            recovery_pending = False
            try:
                while not self.stop.is_set():
                    try:
                        if recovery_pending:
                            self.recover_interrupted()
                            recovery_pending = False
                        job_id = self.claim()
                        if job_id is None:
                            self.stop.wait(self.config["BATCH_WORKER_INTERVAL"])
                            continue
                        self.process(job_id)
                        remove_spool(self.spool, job_id)
                    except Exception:
                        # Например, PostgreSQL отключился после записи файла.
                        # При восстановлении связи сохраняем подтверждённые строки
                        # и помечаем незавершённую задачу как interrupted.
                        log.exception("Сбой обработки очереди, повторная проверка после паузы")
                        recovery_pending = True
                        self.stop.wait(self.config["BATCH_WORKER_INTERVAL"])
            finally:
                self.stop.set()

    def clean_history(self) -> None:
        """Раз в минуту удаляет просроченные записи завершённых задач.

        Использует BATCH_RETENTION_HOURS и ведёт журнал результата.
        Работает в своём потоке, поэтому очистка идёт и во время долгой
        записи в Samba. При ошибке PostgreSQL повторяет попытку через минуту.
        """

        while not self.stop.is_set():
            try:
                expired = self.db.delete_expired_jobs(self.config["BATCH_RETENTION_HOURS"])
                if expired:
                    log.info("Удалено просроченных записей о загрузках: %s", expired)
            except SQLAlchemyError:
                log.exception("Не удалось очистить историю загрузок")
            self.stop.wait(60)

    def recover_interrupted(self) -> None:
        """Восстанавливает состояние очереди при старте или после сбоя цикла.

        Помечает validating, uploading и cancelling как interrupted.
        Сохраняет уже подтверждённые результаты файлов. После фиксации
        транзакции удаляет временные ZIP этих задач. Задачи queued остаются
        доступны следующему проходу воркера.
        """

        with self.db.session.begin() as session:
            jobs = session.scalars(select(Job).where(Job.status.in_(IN_PROGRESS)).with_for_update()).all()
            ids = [job.id for job in jobs]
            for job in jobs:
                finish_job(session, job, "interrupted", "Обработчик был остановлен во время загрузки")
        for job_id in ids:
            remove_spool(self.spool, job_id)

    def claim(self) -> UUID | None:
        """Берёт старейшую доступную задачу queued в короткой транзакции.

        Блокирует выбранную строку, переводит её в validating и обновляет
        ревизию SSE. Возвращает UUID задачи или None, если очередь пуста.
        """

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
            return job.id

    def prepare(self, job_id: UUID, entries: list[Entry]) -> None:
        """Создаёт строки JobFile по результатам сопоставления с data.json.

        Сохраняет путь файла, размер и запись из JSON в meta. Готовые к
        записи файлы получают pending, ошибки сопоставления или превышение
        лимита файла сразу получают error. Устанавливает общее число строк,
        начальные счётчики и статус uploading. Если задача уже отменяется,
        создание строк пропускается.
        """

        with self.db.session.begin() as session:
            job = session.get(Job, job_id, with_for_update=True)
            if job.status == "cancelling":
                return
            errors = 0
            for position, entry in enumerate(entries):
                reason = entry.error
                if (
                    reason is None
                    and entry.member is not None
                    and entry.member.file_size > self.config["BATCH_MAX_FILE_BYTES"]
                ):
                    reason = "file_too_large"
                session.add(
                    JobFile(
                        id=uuid4(),
                        job_id=job_id,
                        position=position,
                        filename=entry.filename,
                        archive_path=entry.member.filename if entry.member else None,
                        size=entry.member.file_size if entry.member else None,
                        meta=entry.meta,
                        status="error" if reason else "pending",
                        reason=reason,
                    )
                )
                errors += bool(reason)
            job.total = len(entries)
            # Ошибки проверки уже имеют итоговый статус и входят в прогресс.
            job.processed = job.errors = errors
            job.status = "uploading"
            job.updated_at = now()
            bump_revision(session)

    def take_file(self, job_id: UUID) -> WorkFile | None:
        """Выбирает следующий файл pending и фиксирует статус processing.

        Сначала проверяет отмену, завершение всей задачи и остановку
        воркера. При любом из этих итогов завершает задачу и возвращает None.
        Для очередного файла возвращает WorkFile с данными, нужными после
        выхода из транзакции. Сама запись в Samba выполняется в process().
        """

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
            # Берём метаданные той же строки, пока сессия открыта.
            return WorkFile(item.id, item.archive_path, item.size, meta=item.meta or {})

    def record_file(self, job_id: UUID, file_id: UUID, status: str, reason: str | None) -> None:
        """Сохраняет итог одного файла и пересчитывает сводку задачи.

        Записывает переданные status и reason, увеличивает processed и
        соответствующий счётчик uploaded, exists или errors. В той же
        транзакции обновляет время задачи и ревизию для фронтенда.
        """

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

    def _save_file(self, archive: ZipFile, item: WorkFile) -> tuple[str, str | None, object]:
        """Передаёт распакованные байты функции записи основного сервиса.

        prepare() уже ограничил размер. В буфере позиция установлена на
        начало, а name содержит путь внутри ZIP. Возвращает статус,
        причину и значение, которое вернула save_file. Ошибка чтения ZIP
        учитывается отдельно от исключений функции записи.
        """

        with BytesIO() as contents:
            try:
                with archive.open(item.archive_path) as source:
                    shutil.copyfileobj(source, contents, length=CHUNK_SIZE)
                if contents.tell() != item.size:
                    raise BadZipFile("Размер файла отличается от указанного в ZIP")
            except (BadZipFile, NotImplementedError, OSError):
                return "error", "invalid_zip_member", None

            contents.seek(0)
            contents.name = item.archive_path
            try:
                save_result = self.save_file(contents)
            except (FileExistsError, self.file_exists_error):
                return "exists", None, None
            except Exception:
                log.exception("Ошибка записи файла %s", item.id)
                return "error", "storage_error", None
            return "uploaded", None, save_result

    def process(self, job_id: UUID) -> None:
        """Проверяет архив, отправляет файлы в Samba и сохраняет их результаты.

        Читает ZIP и data.json через inspect_zip(), создаёт строки через
        prepare() и обрабатывает файлы по одному. Пользовательская функция
        записи вызывается без открытой транзакции PostgreSQL. Ошибка
        отдельного файла сохраняется в его строке. Ошибка самого архива
        завершает задачу через fail().
        """

        try:
            with ZipFile(self.spool / str(job_id) / "archive.zip") as archive:
                entries = inspect_zip(
                    archive,
                    max_members=self.config["BATCH_MAX_MEMBERS"],
                    max_unpacked=self.config["BATCH_MAX_UNPACKED_BYTES"],
                    max_manifest=self.config["BATCH_MAX_MANIFEST_BYTES"],
                )
                # Здесь доступны все совпадения файлов с записями из data.json.
                self.prepare(job_id, entries)
                while (item := self.take_file(job_id)) is not None:
                    status, reason, save_result = self._save_file(archive, item)
                    # Функция приложения получает item.meta и результат Samba
                    # только для нового файла после подтверждённой записи.
                    if status == "uploaded" and self.after_save is not None:
                        try:
                            self.after_save(item, save_result)
                        except Exception:
                            log.exception("Ошибка после записи файла %s", item.id)
                            status, reason = "error", "post_save_error"
                    self.record_file(job_id, item.id, status, reason)
        except ArchiveError as exc:
            self.fail(job_id, str(exc))
        except (BadZipFile, NotImplementedError):
            self.fail(job_id, "Повреждённый ZIP или неподдерживаемый метод сжатия")
        except OSError:
            log.exception("Не удалось прочитать временный архив %s", job_id)
            self.fail(job_id, "Не удалось прочитать временный архив")

    def fail(self, job_id: UUID, error: str) -> None:
        """Завершает задачу с общей ошибкой или подтверждает запрошенную отмену.

        Повторно читает статус под блокировкой. Если пришла отмена, ставит
        cancelled. Иначе передаёт ошибку в finish_job() и фиксирует failed.
        """

        with self.db.session.begin() as session:
            job = session.get(Job, job_id, with_for_update=True)
            if job.status == "cancelling":
                finish_job(session, job, "cancelled")
            else:
                finish_job(session, job, "failed", error)
