from server import app


if __name__ == "__main__":
    # Прямой запуск для локальной проверки. В Docker приложение загружает Gunicorn.
    app.run(host="0.0.0.0", port=8000)
