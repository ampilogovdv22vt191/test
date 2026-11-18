import argparse
import logging
import os

from flask import Flask
from werkzeug.utils import import_string

from . import Batch, create_app
from .worker import Worker


def _load_app() -> Flask:
    """Загружает приложение сервиса, если воркеру нужны его настройки и адаптеры."""

    target = os.environ.get("BATCH_FLASK_APP")
    if not target:
        return create_app()
    app_or_factory = import_string(target)
    app = app_or_factory() if not isinstance(app_or_factory, Flask) else app_or_factory
    if not isinstance(app, Flask):
        raise TypeError("BATCH_FLASK_APP должен указывать на приложение Flask или его фабрику")
    return app


def main() -> None:
    """Запускает одноразовое создание таблиц или постоянный обработчик очереди."""

    parser = argparse.ArgumentParser(description="Пакетные загрузки Flask")
    parser.add_argument("command", choices=("init-db", "worker"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    app = _load_app()
    batch: Batch | None = app.extensions.get("batch_upload")
    if batch is None:
        parser.error("Подключите init_batch(app) в приложении из BATCH_FLASK_APP")
    if args.command == "init-db":
        batch.db.init_schema()
        logging.info("Таблицы пакетной загрузки готовы")
    else:
        Worker(batch.db, batch.storages, app.config).run()


if __name__ == "__main__":
    main()
