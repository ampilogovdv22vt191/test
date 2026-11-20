from io import BytesIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from unittest import TestCase
from unittest.mock import MagicMock, Mock
from uuid import uuid4
from zipfile import ZipFile

from flask import Flask, request

from batch_upload import init_batch
from batch_upload.archive import inspect_zip
from batch_upload.worker import WorkFile, Worker


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
        )
        self.batch = init_batch(self.app, save_file=Mock())
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
            first = Worker(db, Mock(), {"BATCH_WORK_DIR": spool, "BATCH_WORKER_INTERVAL": 0.1})
            second = Worker(db, Mock(), first.config)
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



class SambaFileExists(Exception):
    """Пример исключения, которое поднимает существующая библиотека."""

    pass


class SambaCallbackTest(TestCase):
    def setUp(self):
        """Создаёт небольшой ZIP с настоящим data.json и файлом внутри папки."""

        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.spool = Path(temporary.name)
        self.job_id = uuid4()
        self.key = "docs/report.txt"
        self.payload = b"file contents"
        self.item = WorkFile(uuid4(), self.key, len(self.payload))
        folder = self.spool / str(self.job_id)
        folder.mkdir()
        self.archive = folder / "archive.zip"
        with ZipFile(self.archive, "w") as zip_file:
            zip_file.writestr("data.json", json.dumps([{"filename": "report.txt"}]))
            zip_file.writestr(self.key, self.payload)

        self.save = Mock()
        self.db = Mock()
        self.config = {
            "BATCH_WORK_DIR": self.spool,
            "BATCH_MAX_MEMBERS": 20,
            "BATCH_MAX_UNPACKED_BYTES": 4096,
            "BATCH_MAX_MANIFEST_BYTES": 4096,
            "BATCH_MAX_FILE_BYTES": 4096,
        }
        self.worker = Worker(self.db, self.save, self.config, file_exists_error=SambaFileExists)

    def _run_job(self):
        """Запускает обработку ZIP с подменой только операций PostgreSQL."""

        self.worker.prepare = Mock()
        self.worker.take_file = Mock(side_effect=[self.item, None])
        self.worker.record_file = Mock()
        self.worker.process(self.job_id)

    def test_take_file_passes_metadata_to_the_worker(self):
        """Проверяет поля data.json и пустой словарь для старой строки без meta."""

        self.db.session = MagicMock()
        session = self.db.session.begin.return_value.__enter__.return_value
        session.get.return_value = Mock(status="uploading", processed=0, total=1)
        metadata = {"filename": "report.txt", "city": "Москва", "country": "Россия", "village": "Пример"}

        for stored_meta, expected in ((metadata, metadata), (None, {})):
            with self.subTest(stored_meta=stored_meta):
                row = Mock(id=self.item.id, archive_path=self.key, size=self.item.size, meta=stored_meta)
                session.scalars.return_value.one.return_value = row

                item = self.worker.take_file(self.job_id)

                self.assertEqual(row.status, "processing")
                self.assertEqual(item.meta, expected)

    def test_passes_bytesio_with_the_relative_name(self):
        """Функция Samba получает открытый BytesIO на первом байте и относительный путь."""

        def save(contents):
            """Проверяет контракт перед тем, как воркер закроет поток."""

            self.assertIsInstance(contents, BytesIO)
            self.assertEqual(contents.name, self.key)
            self.assertEqual(contents.tell(), 0)
            self.assertEqual(contents.read(), self.payload)

        self.save.side_effect = save
        self._run_job()

        self.save.assert_called_once()
        self.worker.record_file.assert_called_once_with(self.job_id, self.item.id, "uploaded", None)

    def test_custom_and_builtin_exists_exceptions(self):
        """Оба вида исключения о существующем файле дают статус exists."""

        for exception in (SambaFileExists, FileExistsError):
            with self.subTest(exception=exception):
                self.save.side_effect = exception
                self._run_job()
                self.worker.record_file.assert_called_once_with(self.job_id, self.item.id, "exists", None)

    def test_samba_failure_is_recorded_for_the_file(self):
        """Сбой записи сохраняется в статусе конкретного файла."""

        self.save.side_effect = OSError("samba unavailable")
        with self.assertLogs("batch_upload.worker", level="ERROR"):
            self._run_job()
        self.worker.record_file.assert_called_once_with(
            self.job_id, self.item.id, "error", "storage_error"
        )

    def test_file_limit_is_applied_before_the_archive_member_is_read(self):
        """Превышение лимита отдельного файла исключает его из очереди записи."""

        self.config["BATCH_MAX_FILE_BYTES"] = 1
        self.db.session = MagicMock()
        session = self.db.session.begin.return_value.__enter__.return_value
        session.get.return_value = Mock(status="validating")
        with ZipFile(self.archive) as zip_file:
            entries = inspect_zip(zip_file, max_members=20, max_unpacked=4096, max_manifest=4096)
        self.worker.prepare(self.job_id, entries)

        stored = session.add.call_args.args[0]
        self.assertEqual(stored.status, "error")
        self.assertEqual(stored.reason, "file_too_large")
        self.save.assert_not_called()


class JobFilesTest(TestCase):
    def test_batch_api_keeps_job_details_and_has_no_file_download(self):
        """Проверяет детали задачи и сохранение собственного маршрута скачивания."""

        with TemporaryDirectory() as spool:
            app = Flask(__name__)
            app.config["BATCH_WORK_DIR"] = spool

            @app.get("/my-download/<name>")
            def own_download(name):
                """Имитирует ручку скачивания действующего приложения."""

                return name

            init_batch(app, save_file=Mock())
            rules = {rule.rule for rule in app.url_map.iter_rules()}
            self.assertIn("/upload/batch/<uuid:job_id>", rules)
            self.assertNotIn("/upload/files/<uuid:file_id>", rules)
            self.assertEqual(app.test_client().get("/my-download/report.txt").data, b"report.txt")
