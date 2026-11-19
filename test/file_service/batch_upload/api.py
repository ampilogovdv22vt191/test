from datetime import timedelta
import logging
from pathlib import Path
import time
from uuid import uuid4

from flask import Blueprint, Response, current_app, jsonify, request, send_file, url_for
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload
from werkzeug.exceptions import BadRequest, RequestEntityTooLarge

from .db import ACTIVE, READABLE, Job, JobFile, Revision, bump_revision, now
from .storage import CHUNK_SIZE
from .worker import finish_job, remove_spool


api = Blueprint("batch_upload", __name__)
log = logging.getLogger(__name__)


def configure_request_limits(app) -> None:
    """Устанавливает отдельные лимиты для POST /upload/batch во Flask 2.2.

    Наследует текущий класс запроса приложения. Для маршрута пакетной
    загрузки задаёт предел тела запроса и настройки разбора multipart.
    Остальные маршруты используют лимиты исходного класса запроса.
    """

    upload_endpoint = f"{api.name}.upload_batch"

    class BatchRequest(app.request_class):
        @property
        def max_content_length(self):
            """Возвращает предел всего HTTP-тела в байтах для текущего маршрута."""

            if self.endpoint == upload_endpoint:
                # Запас для заголовков формы. Лимит самого ZIP проверяет upload_batch.
                return app.config["BATCH_MAX_ARCHIVE_BYTES"] + 256 * 1024
            return super().max_content_length

        def make_form_data_parser(self):
            """Создаёт парсер формы и ограничивает число и размер её частей.

            Для загрузки ZIP допускает до четырёх частей multipart.
            Буфер парсера ограничен 500 000 байтами.
            """

            parser = super().make_form_data_parser()
            if self.endpoint == upload_endpoint:
                parser.max_form_parts = 4
                parser.max_form_memory_size = 500_000
            return parser

    app.request_class = BatchRequest


def _components():
    """Берёт объект Batch из текущего Flask-приложения.

    В Batch находятся фабрика сессий PostgreSQL, каталог временных ZIP
    и адаптеры хранилища. Объект создаёт init_batch() при настройке Flask.
    """

    return current_app.extensions["batch_upload"]


def _job_json(job: Job) -> dict:
    """Преобразует запись Job из PostgreSQL в сводку для фронтенда.

    UUID становится строкой, а даты — строками ISO 8601. Сводка содержит
    статус архива, его общую ошибку и счётчики. total равен None до проверки
    архива, а finished_at — до завершения. Список файлов добавляет job_detail().
    """

    return {
        "job_id": str(job.id),
        "archive_name": job.archive_name,
        "status": job.status,
        "total": job.total,
        "processed": job.processed,
        "uploaded": job.uploaded,
        "exists": job.exists,
        "errors": job.errors,
        "error": job.error,
        "created_at": job.created_at.isoformat(),
        "updated_at": job.updated_at.isoformat(),
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
    }


def _file_json(item: JobFile) -> dict:
    """Преобразует одну строку JobFile в словарь для ответа с деталями задачи.

    file_id — UUID строки в PostgreSQL, filename — имя файла без каталога,
    archive_path — путь внутри ZIP и ключ объекта в хранилище. Если файл
    не найден при сопоставлении, путь и размер могут отсутствовать.
    size хранит размер файла из ZIP в байтах, meta — его запись из data.json.

    status показывает результат обработки, reason содержит причину ошибки
    или отмены. Для статусов uploaded и exists добавляется download_url
    на маршрут скачивания. Для остальных статусов ссылка равна None.
    """

    return {
        "file_id": str(item.id),
        "filename": item.filename,
        "archive_path": item.archive_path,
        "size": item.size,
        "meta": item.meta,
        "status": item.status,
        "reason": item.reason,
        "download_url": (
            url_for(".download_file", file_id=item.id) if item.status in READABLE else None
        ),
    }


@api.errorhandler(BadRequest)
@api.errorhandler(RequestEntityTooLarge)
def invalid_request(error):
    """Преобразует ошибку входного запроса в JSON с исходным HTTP-статусом.

    BadRequest даёт 400, а превышение лимита тела или формы даёт 413.
    Текст из исключения передаётся фронтенду в поле error.
    """

    return jsonify(error=error.description), error.code


@api.errorhandler(SQLAlchemyError)
def db_error(error):
    """Записывает подробности ошибки SQLAlchemy в журнал и возвращает JSON с 503."""

    log.error("Ошибка базы данных пакетных загрузок", exc_info=error)
    return jsonify(error="База данных загрузок временно недоступна"), 503


@api.post("/upload/batch")
def upload_batch():
    """Принимает один архив из поля archive в форме multipart.

    Порциями записывает его в каталог BATCH_WORK_DIR/<job_id> и проверяет
    размер самого ZIP. После записи создаёт задачу queued и увеличивает
    ревизию SSE в одной транзакции. Возвращает 202 и ссылку на детали
    в заголовке Location. Содержимое архива затем проверяет воркер.
    """

    components = _components()
    limit = current_app.config["BATCH_MAX_ARCHIVE_BYTES"]
    submitted = request.files.getlist("archive")
    if len(submitted) != 1 or not submitted[0].filename:
        raise BadRequest("Передайте один ZIP в поле archive")
    archive = submitted[0]
    # Имя, присланное браузером, используется только для отображения.
    archive_name = archive.filename.replace("\\", "/").rsplit("/", 1)[-1]
    archive_name = "".join(char for char in archive_name if ord(char) >= 32)[:255] or "archive.zip"

    job_id = uuid4()
    folder = components.spool / str(job_id)
    folder.mkdir(mode=0o700)
    try:
        with (folder / "archive.zip").open("xb") as out:
            size = 0
            while chunk := archive.stream.read(CHUNK_SIZE):
                size += len(chunk)
                if size > limit:
                    raise RequestEntityTooLarge("ZIP превышает допустимый размер")
                out.write(chunk)
        if size == 0:
            raise BadRequest("Архив пуст")
    except Exception:
        remove_spool(components.spool, job_id)
        raise

    # Воркер увидит задачу только после сохранения ZIP целиком.
    # Проверки содержимого файлов и обращения к Samba добавляй в воркер,
    # чтобы POST возвращал 202 сразу после принятия архива в очередь.
    # Если БД не подтвердит фиксацию транзакции, сохраняем ZIP. Запись могла создаться.
    with components.db.session.begin() as session:
        session.add(Job(id=job_id, archive_name=archive_name, backend=components.default_backend))
        bump_revision(session)
    location = url_for(".job_detail", job_id=job_id)
    return jsonify(job_id=str(job_id), status="queued"), 202, {"Location": location}


@api.get("/upload/batch")
def list_jobs():
    """Возвращает сводки для списка загрузок на фронтенде.

    Принимает hours от 1 до 168, по умолчанию использует BATCH_HISTORY_HOURS.
    Активные задачи попадают в список независимо от возраста. Завершённые
    отбираются по finished_at. Результат отсортирован от новых к старым.
    """

    hours = request.args.get("hours", default=str(current_app.config["BATCH_HISTORY_HOURS"]))
    try:
        hours = int(hours)
    except ValueError:
        raise BadRequest("hours должен быть целым числом от 1 до 168") from None
    if not 1 <= hours <= 168:
        raise BadRequest("hours должен быть целым числом от 1 до 168")
    with _components().db.session() as session:
        jobs = session.scalars(
            select(Job)
            .where(or_(Job.status.in_(ACTIVE), Job.finished_at >= now() - timedelta(hours=hours)))
            .order_by(Job.created_at.desc(), Job.id.desc())
        )
        return jsonify(window_hours=hours, jobs=[_job_json(job) for job in jobs])


@api.get("/upload/batch/<uuid:job_id>")
def job_detail(job_id):
    """Возвращает состояние одной загрузки по UUID из URL.

    Запрашивает задачу и её файлы вместе. Собирает сводку через _job_json(),
    а массив files — через _file_json(). Для отсутствующей или удалённой
    задачи возвращает 404.
    """

    with _components().db.session() as session:
        job = session.execute(
            select(Job).options(joinedload(Job.files)).where(Job.id == job_id)
        ).unique().scalar_one_or_none()
        if job is None:
            return jsonify(error="Загрузка не найдена"), 404
        return jsonify(**_job_json(job), files=[_file_json(item) for item in job.files])


@api.post("/upload/batch/<uuid:job_id>/cancel")
def cancel_job(job_id):
    """Запрашивает отмену задачи по UUID и блокирует её строку в БД.

    Для queued сразу фиксирует cancelled, удаляет временный ZIP после
    завершения транзакции и возвращает 200. Для активной задачи ставит
    cancelling и возвращает 202. Воркер завершит текущий файл и остановится.
    Для отсутствующей задачи ответ 404. При повторной отмене уже отменённой
    задачи ответ 200, для остальных итоговых статусов — 409.
    """

    components = _components()
    with components.db.session.begin() as session:
        # Воркер блокирует ту же строку, когда берёт задачу или начинает писать файл.
        job = session.get(Job, job_id, with_for_update=True)
        if job is None:
            return jsonify(error="Загрузка не найдена"), 404
        if job.status == "queued":
            finish_job(session, job, "cancelled")
        elif job.status in ("validating", "uploading"):
            job.status = "cancelling"
            job.updated_at = now()
            bump_revision(session)
        elif job.status not in ("cancelling", "cancelled"):
            return jsonify(error="Загрузка уже завершена", status=job.status), 409
        status = job.status
    # Удалять ZIP можно после фиксации отмены. Активный ZIP ещё нужен воркеру.
    if status == "cancelled":
        remove_spool(components.spool, job_id)
    return jsonify(job_id=str(job_id), status=status), (200 if status == "cancelled" else 202)


@api.get("/upload/events")
def upload_events():
    """Открывает поток SSE для модалки со списком загрузок.

    Возвращает text/event-stream. Событие sync сообщает, что фронтенду
    пора заново запросить список загрузок и детали выбранной задачи.
    URL и данные для этих запросов определяет фронтенд.
    """

    db = _components().db
    interval = current_app.config["BATCH_SSE_INTERVAL"]

    def stream():
        """Читает ревизию из БД и выдаёт SSE-сообщения по одному.

        На первом чтении и при изменении ревизии отправляет sync.
        Каждые 15 секунд отправляет heartbeat. Сессия БД закрывается
        между чтениями. При ошибке БД браузер сможет переподключиться
        и получить свежую ревизию.
        """

        previous = None
        heartbeat_at = time.monotonic() + 15
        while True:
            try:
                # Сессия БД закрывается до отправки события и ожидания.
                with db.session() as session:
                    revision = session.scalar(select(Revision.value).where(Revision.id == 1))
                    if revision is None:
                        raise RuntimeError("Batch schema has not been initialized")
            except (SQLAlchemyError, RuntimeError):
                # Браузер переподключится. Первое событие запустит чтение свежих данных.
                log.exception("SSE потерял соединение с базой загрузок")
                return
            if revision != previous:
                previous = revision
                yield f'event: sync\ndata: {{"revision": "{revision}"}}\n\n'
            if time.monotonic() >= heartbeat_at:
                heartbeat_at = time.monotonic() + 15
                yield ": heartbeat\n\n"
            time.sleep(interval)

    return Response(
        stream(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"},
    )


@api.get("/upload/files/<uuid:file_id>")
def download_file(file_id):
    """Отдаёт сохранённый файл как вложение по UUID его строки JobFile.

    По записи из БД выбирает адаптер хранилища, относительный ключ и имя
    для скачивания. Отсутствующая запись или файл дают 404, ошибка связи
    с хранилищем даёт 503. Поток передаётся Flask для отправки клиенту.
    """

    components = _components()
    with components.db.session() as session:
        row = session.execute(
            select(Job.backend, JobFile.archive_path, JobFile.filename)
            .join(JobFile, Job.id == JobFile.job_id)
            .where(JobFile.id == file_id, JobFile.status.in_(READABLE))
        ).one_or_none()
    if row is None:
        return jsonify(error="Файл недоступен для скачивания"), 404
    storage = components.storages.get(row.backend)
    if storage is None:
        return jsonify(error="Хранилище этого файла недоступно"), 503
    try:
        source = storage.open_read(row.archive_path)
    except FileNotFoundError:
        return jsonify(error="Файл отсутствует в хранилище"), 404
    except OSError:
        log.exception("Не удалось открыть файл %s", file_id)
        return jsonify(error="Не удалось открыть файл в хранилище"), 503
    try:
        response = send_file(
            source,
            mimetype="application/octet-stream",
            as_attachment=True,
            download_name=row.filename,
            conditional=False,
        )
    except Exception:
        source.close()
        raise
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response
