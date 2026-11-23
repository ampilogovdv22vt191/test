FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# Веб-сервис и обработчик работают с томом под одним пользователем.
RUN groupadd -g 10001 app && useradd -u 10001 -g app app \
    && install -d -o app -g app /batch-work

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY run.py gunicorn.conf.py ./
COPY server/ ./server/
COPY batch_upload/ ./batch_upload/

USER app
EXPOSE 5000
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s \
    CMD ["python", "-c", "from urllib.request import urlopen; urlopen('http://127.0.0.1:5000/health', timeout=2).close()"]

# Потоки обслуживают обычные запросы, пока браузеры держат открытые SSE-соединения.
CMD ["gunicorn", "--config", "gunicorn.conf.py", "run:app"]
