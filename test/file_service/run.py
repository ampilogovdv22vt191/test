import signal

from batch_upload import start_worker
from server import app


def _exit_on_term(signum, frame):
    """Обрабатывает SIGTERM при прямом запуске через python run.py.

    SystemExit завершает веб-сервер через finally, где процесс просит
    фоновый обработчик остановиться.
    """

    raise SystemExit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _exit_on_term)
    processor = start_worker(app)
    try:
        # Перезагрузчик запустил бы второй процесс с собственным обработчиком.
        app.run(host="0.0.0.0", port=5000, threaded=True, use_reloader=False)
    finally:
        processor.close()
