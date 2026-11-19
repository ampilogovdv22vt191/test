from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from unittest import TestCase
from unittest.mock import MagicMock, Mock

from flask import Flask, request

from batch_upload import init_batch
from batch_upload.worker import Worker


class UploadLimitsTest(TestCase):
    def setUp(self):
        """Готовит Flask с временным каталогом и подменёнными БД и хранилищем."""

        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.spool = Path(temporary.name)
        self.app = Flask(__name__)
        self.app.config.update(
            TESTING=True,
            MAX_CONTENT_LENGTH=512,
            BATCH_MAX_ARCHIVE_BYTES=4096,
            BATCH_WORK_DIR=self.spool,
            BATCH_STORAGE_BACKEND="test",
        )
        self.batch = init_batch(self.app, storages={"test": Mock()})
        self.batch.db.session = MagicMock()

        @self.app.post("/existing")
        def existing():
            """Имитирует старый маршрут, который читает всё тело запроса."""

            return {"size": len(request.get_data())}

        self.client = self.app.test_client()

    def test_batch_limit_is_independent_of_the_existing_app_limit(self):
        """Проверяет, что пакетный маршрут принимает тело крупнее старого лимита."""

        body = b"x" * 1024
        result = self.client.post("/upload/batch", data={"archive": (BytesIO(body), "files.zip")})
        self.assertEqual(result.status_code, 202)
        job_id = result.json["job_id"]
        self.assertEqual((self.spool / job_id / "archive.zip").read_bytes(), body)
        self.assertEqual(self.client.post("/existing", data=body).status_code, 413)

    def test_archive_limit_cleans_spool_and_does_not_create_job(self):
        """Проверяет ответ 413 и уборку временных данных при слишком большом ZIP."""

        result = self.client.post("/upload/batch", data={"archive": (BytesIO(b"x" * 4097), "files.zip")})
        self.assertEqual(result.status_code, 413)
        self.assertEqual(list(self.spool.iterdir()), [])
        self.batch.db.session.begin.assert_not_called()

    def test_limits_the_entire_request_and_multipart_parts(self):
        """Проверяет предел всего HTTP-тела и максимальное число частей формы."""

        large = BytesIO(b"x" * (4096 + 256 * 1024))
        self.assertEqual(
            self.client.post("/upload/batch", data={"archive": (large, "files.zip")}).status_code,
            413,
        )
        multipart = {"archive": (BytesIO(b"x"), "files.zip")}
        multipart.update({f"field{index}": "x" for index in range(4)})
        self.assertEqual(self.client.post("/upload/batch", data=multipart).status_code, 413)
        self.batch.db.session.begin.assert_not_called()


class WorkerStartupTest(TestCase):
    def test_initializes_then_runs_in_thread_and_releases_the_lock(self):
        """Проверяет подготовку таблиц, запуск и передачу блокировки после остановки."""

        with TemporaryDirectory() as spool:
            db = Mock()
            first = Worker(db, {}, {"BATCH_WORK_DIR": spool, "BATCH_WORKER_INTERVAL": 0.1})
            second = Worker(db, {}, first.config)
            started = Event()
            first.clean_history = Mock()
            first.recover_interrupted = Mock()
            first.claim = Mock(side_effect=started.set)
            second.clean_history = Mock()
            second.recover_interrupted = Mock()
            second.claim = Mock()
            first.start()
            try:
                self.assertTrue(started.wait(2))
                db.init_schema.assert_called_once_with()
                first.recover_interrupted.assert_called_once_with()
                with self.assertRaises(BlockingIOError):
                    second.start()
            finally:
                first.close()
            self.assertFalse(first.thread.is_alive())
            second.start()
            second.close()
            self.assertEqual(db.init_schema.call_count, 2)
