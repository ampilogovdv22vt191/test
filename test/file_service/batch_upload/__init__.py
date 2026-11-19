from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path

from flask import Flask
from sqlalchemy.engine import URL, make_url

from .api import api, configure_request_limits
from .db import Database
from .storage import MountedStorage, Storage
from .worker import Worker


DEFAULTS = {
    "BATCH_WORK_DIR": "./batch-spool",
    "BATCH_STORAGE_DIR": "./storage",
    "BATCH_STORAGE_BACKEND": "samba",
    "BATCH_MAX_ARCHIVE_BYTES": 256 * 1024**2,
    "BATCH_MAX_UNPACKED_BYTES": 2 * 1024**3,
    "BATCH_MAX_MEMBERS": 5000,
    "BATCH_MAX_MANIFEST_BYTES": 2 * 1024**2,
    "BATCH_HISTORY_HOURS": 8,
    "BATCH_RETENTION_HOURS": 8,
    "BATCH_SSE_INTERVAL": 1.0,
    "BATCH_WORKER_INTERVAL": 1.0,
}


@dataclass(frozen=True)
class Batch:
    """База, общий каталог ZIP и адаптеры, которыми пользуются API и воркер."""

    db: Database
    spool: Path
    storages: dict[str, Storage]
    default_backend: str


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


def _configured_storage(app: Flask) -> dict[str, Storage]:
    """Возвращает зарегистрированный встроенный адаптер хранилища.

    При BATCH_STORAGE_BACKEND=smb читает адрес шары, имя пользователя
    и пароль для прямого SMB-подключения. Для адаптера samba использует
    папку BATCH_STORAGE_DIR, которая может быть примонтированной шарой.
    """

    if app.config["BATCH_STORAGE_BACKEND"] == "smb":
        from .smb import SmbStorage

        credentials = {}
        for setting in ("ROOT", "USERNAME", "PASSWORD"):
            key = f"BATCH_SMB_{setting}"
            value = app.config.get(key, os.environ.get(key))
            if not value:
                raise ValueError(f"Для прямого подключения к Samba задайте {key}")
            credentials[setting.lower()] = value
        return {"smb": SmbStorage(**credentials)}
    return {"samba": MountedStorage(app.config["BATCH_STORAGE_DIR"])}


def init_batch(app: Flask, *, storages: Mapping[str, Storage] | None = None) -> Batch:
    """Подключает маршруты пакетной загрузки к переданному экземпляру Flask.

    Загружает и проверяет настройки, создаёт каталог временных ZIP,
    выбирает адаптеры и сохраняет объект Batch в app.extensions.
    Переданная карта storages позволяет подключить свои адаптеры.
    Здесь же регистрируются отдельные лимиты запроса и Blueprint.
    """

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
    # Можно передать несколько адаптеров, чтобы читать и старые, и новые загрузки.
    backends = _configured_storage(app) if storages is None else dict(storages)
    default_backend = app.config["BATCH_STORAGE_BACKEND"]
    if default_backend not in backends:
        raise ValueError(f"Batch storage backend {default_backend!r} is not configured")
    batch = Batch(Database(_database_url(app)), spool, backends, default_backend)
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
    worker = Worker(batch.db, batch.storages, app.config)
    worker.start()
    return worker
