FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# Все три контейнера работают с общим каталогом под одним пользователем.
RUN groupadd -g 10001 app && useradd -u 10001 -g app app \
    && install -d -o app -g app /batch-work

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY run.py ./
COPY server/ ./server/
COPY batch_upload/ ./batch_upload/

USER app
EXPOSE 8000

# Потоки обслуживают обычные запросы, пока браузеры держат открытые SSE-соединения.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "16", "--timeout", "120", "run:app"]
