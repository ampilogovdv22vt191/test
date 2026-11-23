from batch_upload import start_worker


bind = "0.0.0.0:5000"
workers = 1
threads = 16
timeout = 120


def post_worker_init(worker):
    """Запускает обработчик после загрузки Flask в веб-процессе Gunicorn.

    Обработчик становится частью этого процесса. Ссылку на него сохраняем
    в объекте Gunicorn, чтобы дождаться остановки в worker_exit().
    """

    worker.batch_processor = start_worker(worker.wsgi)


def worker_exit(server, worker):
    """Просит фоновый поток завершить работу при выходе веб-процесса.

    Если приложение завершилось до запуска обработчика, закрывать нечего.
    """

    processor = getattr(worker, "batch_processor", None)
    if processor is not None:
        processor.close()
