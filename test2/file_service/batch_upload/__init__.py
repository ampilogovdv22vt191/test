from dataclasses import dataclass
from io import BytesIO
import os
from pathlib import Path

from flask import Flask
from sqlalchemy.engine import URL, make_url

from .api import api, configure_request_limits
from .db import Database
from .storage import SaveFile
from .worker import Worker


DEFAULTS = {
    "BATCH_WORK_DIR": "./batch-spool",
    "BATCH_MAX_ARCHIVE_BYTES": 256 * 1024**2,
    "BATCH_MAX_UNPACKED_BYTES": 2 * 1024**3,
    "BATCH_MAX_FILE_BYTES": 256 * 1024**2,
    "BATCH_MAX_MEMBERS": 5000,
    "BATCH_MAX_MANIFEST_BYTES": 2 * 1024**2,
    "BATCH_HISTORY_HOURS": 8,
    "BATCH_RETENTION_HOURS": 8,
    "BATCH_SSE_INTERVAL": 1.0,
    "BATCH_WORKER_INTERVAL": 1.0,
}


@dataclass(frozen=True)
class Batch:
    """База, каталог ZIP и функция записи, которую передал основной сервис."""

    db: Database
    spool: Path
    save_file: SaveFile
    file_exists_error: type[Exception]


def _database_url(app: Flask) -> URL:
    """Возвращает адрес подключения SQLAlchemy к PostgreSQL.

    Сначала берёт BATCH_DATABASE_URL из Flask или окружения. Для адреса
    postgresql:// явно выбирает драйвер psycopg. Если полного адреса
    нет, собирает его из отдельных переменных POSTGRES_*.
    """

    configured = app.config.get("BATCH_DATABASE_URL") or os.environ.get("BATCH_DATABASE_URL")
    if configured:
        url = make_url(configured)
        return url.set(drivername="postgresql+psycopg") if url.drivername == "postgresql" else url
    return URL.create(
        "postgresql+psycopg",
        username=os.environ.get("POSTGRES_USER", "batch_upload"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "batch_upload"),
    )


def init_batch(
    app: Flask,
    *,
    save_file: SaveFile,
    file_exists_error: type[Exception] = FileExistsError,
) -> Batch:
    """Подключает маршруты пакетной загрузки к переданному экземпляру Flask.

    save_file получает BytesIO с данными одного файла и его путём в .name.
    Возвращаемое значение функции игнорируется. Исключение file_exists_error
    означает, что файл уже сохранён. Запись запускает воркер после приёма ZIP.
    """

    if not callable(save_file):
        raise TypeError("save_file должен быть функцией записи")
    if not isinstance(file_exists_error, type) or not issubclass(file_exists_error, Exception):
        raise TypeError("file_exists_error должен быть классом исключения")

    # Настройки существующего приложения имеют приоритет над окружением.
    for key, default in DEFAULTS.items():
        value = app.config.get(key, os.environ.get(key, default))
        if isinstance(default, (int, float)):
            value = type(default)(value)
            if value <= 0:
                raise ValueError(f"{key} must be positive")
        app.config[key] = value
    if app.config["BATCH_HISTORY_HOURS"] > 168:
        raise ValueError("BATCH_HISTORY_HOURS must not exceed 168")

    spool = Path(app.config["BATCH_WORK_DIR"]).resolve()
    spool.mkdir(mode=0o700, parents=True, exist_ok=True)
    app.config["BATCH_WORK_DIR"] = spool
    batch = Batch(Database(_database_url(app)), spool, save_file, file_exists_error)
    app.extensions["batch_upload"] = batch
    configure_request_limits(app)
    app.register_blueprint(api)
    return batch


def start_worker(app: Flask) -> Worker:
    """Запускает фоновый обработчик для приложения, настроенного через init_batch().

    Берёт общие зависимости из app.extensions, передаёт их Worker и
    вызывает start(), который готовит таблицы и запускает поток.
    Возвращает Worker, чтобы веб-сервис мог остановить его при завершении.
    """

    batch: Batch = app.extensions["batch_upload"]
    def save_in_app_context(file: BytesIO) -> None:
        """Вызывает функцию записи с доступом к настройкам и расширениям Flask."""

        with app.app_context():
            batch.save_file(file)

    worker = Worker(batch.db, save_in_app_context, app.config, file_exists_error=batch.file_exists_error)
    worker.start()
    return worker
