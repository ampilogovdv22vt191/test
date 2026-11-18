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


def _components():
    return current_app.extensions["batch_upload"]


def _job_json(job: Job) -> dict:
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
    return jsonify(error=error.description), error.code


@api.errorhandler(SQLAlchemyError)
def db_error(error):
    log.error("Ошибка базы данных пакетных загрузок", exc_info=error)
    return jsonify(error="База данных загрузок временно недоступна"), 503


@api.post("/upload/batch")
def upload_batch():
    """Сохраняет ZIP, создаёт задачу в очереди и возвращает её идентификатор."""

    components = _components()
    limit = current_app.config["BATCH_MAX_ARCHIVE_BYTES"]
    # Даём запас на заголовки multipart. Размер самого ZIP проверяем отдельно.
    request.max_content_length = limit + 256 * 1024
    request.max_form_parts = 4
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
    """Возвращает активные загрузки и завершённые в заданном окне истории."""

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
    """Читает статус задачи и список её файлов одним запросом к БД."""

    with _components().db.session() as session:
        job = session.execute(
            select(Job).options(joinedload(Job.files)).where(Job.id == job_id)
        ).unique().scalar_one_or_none()
        if job is None:
            return jsonify(error="Загрузка не найдена"), 404
        return jsonify(**_job_json(job), files=[_file_json(item) for item in job.files])


@api.post("/upload/batch/<uuid:job_id>/cancel")
def cancel_job(job_id):
    """Отменяет задачу в очереди или сообщает воркеру, что пора остановиться."""

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
    """Сообщает открытой модалке, когда ей нужно перечитать данные."""

    db = _components().db
    interval = current_app.config["BATCH_SSE_INTERVAL"]

    def stream():
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
    """Находит ключ и адаптер по идентификатору файла и отдаёт его потоком."""

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
