# Запуск и интеграция file_service

В каталоге есть минимальное Flask-приложение, Dockerfile и два файла Compose. Они используют существующие PostgreSQL и Samba по сети. Compose сначала запускает `batch_init` для подготовки таблиц в PostgreSQL, после него стартуют веб-контейнер и `batch_worker`. Временные ZIP хранятся в общем локальном томе.

## Запуск готового примера

Из каталога `file_service` создай `.env` из примера. Укажи адрес и доступ к PostgreSQL, путь к сетевой шаре и доступ к Samba.

```bash
cp .env.batch.example .env
# После заполнения .env запусти оба файла Compose.
docker compose -f docker-compose.yml -f compose.batch.yaml up --build -d
```

API будет доступен на `http://127.0.0.1:8000` на хосте Docker. Веб-контейнер запускает Gunicorn с `run:app`, а сам `run.py` импортирует глобальный `app` из `server/__init__.py`. Для прямого запуска `python run.py` вне Docker сначала передай настройки через окружение процесса.

## 1. Перенос в действующий проект

| Файл или каталог | Куда перенести |
|---|---|
| `batch_upload/` | Целиком в корень твоего проекта рядом с `server/` и `run.py`. |
| `server/__init__.py` | В свой файл перенести импорт `init_batch` и вызов `init_batch(app)`. |
| `requirements.txt` | Перенести нужные зависимости в свой список. Gunicorn нужен для запуска из Dockerfile примера. |
| `.env.batch.example` | Добавить настройки в свой `.env` с актуальными адресами и учётными данными. |
| `compose.batch.yaml` | Положить рядом с действующим Compose и подключать вторым файлом. |
| `run.py`, `Dockerfile`, `docker-compose.yml` | Готовые примеры для сверки с твоими действующими файлами. |

Твой `run.py` импортирует глобальный `app` из `server` и запускает Flask. Добавь `init_batch(app)` после создания и настройки `app` в `server/__init__.py`. Оставь в `.env` значение `BATCH_FLASK_APP=server:app`, чтобы воркер и `init-db` могли импортировать тот же объект. Если проверка доступа настроена на отдельных классах `Resource`, подключи ту же проверку к новым маршрутам, включая SSE и скачивание.

Пакет рассчитан на Python 3.11+ и Flask 3.1. Если в текущем проекте закреплена другая версия Flask или SQLAlchemy, согласуй её с зависимостями пакетной загрузки при сборке.

## 2. Дополни свой Dockerfile и Compose

Убедись, что в образ попадает каталог `batch_upload/`. Если Dockerfile перечисляет файлы явно, добавь `COPY batch_upload/ ./batch_upload/` при условии, что рабочий каталог образа уже содержит `run.py` и `server/`.

В `compose.batch.yaml` замени ключ `file_service` под `services` на точное имя текущего веб-сервиса. Укажи в `.env` тег его образа как `FILE_SERVICE_IMAGE`. Compose добавит этому сервису общий том и переменные. Команда запуска и опубликованный порт берутся из исходного Compose. У `batch_worker` своя команда запуска и тот же образ. [Как Compose объединяет файлы](https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/).

Веб-сервис и воркер должны иметь право записи в `/batch-work`. Если образ запускается от непривилегированного пользователя, создай этот каталог в Dockerfile до переключения на `USER` и назначь владельцем пользователя, от которого работает сервис. Например, если это пользователь и группа `app`, используй `RUN install -d -o app -g app /batch-work`. При первом создании именованного тома Docker переносит в него права каталога из образа.

Если у текущего веб-сервиса явно настроены нестандартные сети или нужен `extra_hosts` для доступа к удалённой виртуалке, добавь эти настройки сервисам `batch_init` и `batch_worker`. Из всех трёх контейнеров должен быть доступен PostgreSQL, а из веб-сервиса и воркера — Samba.

## 3. Заполни подключение к PostgreSQL и Samba

- `BATCH_DATABASE_URL` указывает на существующую базу. При первом запуске `batch_init` пользователю БД должно быть разрешено создавать таблицы в выбранной схеме. Специальные символы в логине или пароле внутри URL нужно кодировать как компоненты URL.
- `BATCH_SMB_ROOT` задаёт путь вида `//сервер/шара` или `//сервер/шара/каталог`. В качестве корня укажи ту папку, где пакетная загрузка должна проверять наличие файлов и сохранять новые. Папка должна существовать на Samba.
- `BATCH_SMB_USERNAME` и `BATCH_SMB_PASSWORD` — доступ к Samba. Можно указать доменного пользователя в виде `DOMAIN\\user`. Встроенный адаптер открывает соединение по SMB2/SMB3 на порту 445 и переиспользует его. Учётной записи нужны права читать, создавать, переименовывать и удалять временные файлы. [Подключение через smbclient](https://github.com/jborean93/smbprotocol).

Если твой текущий сервис использует собственный клиент и ты хочешь переиспользовать его, оберни его в два метода из `batch_upload/storage.py`. `put_if_absent` записывает поток и возвращает `uploaded` или `exists`, `open_read` открывает двоичный поток. Передай адаптер в `init_batch(app, storages={"smb": твой_адаптер})`. Веб-сервис, воркер и команда создания таблиц получат этот адаптер через общий `server:app`. С будущим S3 схема такая же. Идентификатор адаптера сохраняется в задаче, чтобы запрос на скачивание выбирал нужное хранилище.

## 4. Запусти с существующим Compose

Из каталога с основным `docker-compose.yml` запусти оба файла одной командой.

```bash
docker compose -f docker-compose.yml -f compose.batch.yaml up --build -d
```

Compose запустит `batch_init` с командой `python -m batch_upload init-db`. Она создаст недостающие таблицы и завершится. Статус `Exited (0)` у `batch_init` означает, что всё готово. Flask и воркер дождутся этого результата. Если создание таблиц завершится ошибкой, причину покажет `docker compose -f docker-compose.yml -f compose.batch.yaml logs batch_init`. [Порядок запуска в Docker Compose](https://docs.docker.com/compose/how-tos/startup-order/).

Если сервис получает готовый образ из реестра, укажи его тег в `FILE_SERVICE_IMAGE` и запусти ту же команду без `--build`.

После запуска попробуй отправить ZIP в `POST /upload/batch` через существующий адрес Flask. В ZIP положи `data.json` в корень, а записи свяжи с файлами через `filename`. Повторная отправка того же файла должна дать `exists`. `GET /upload/batch` покажет ленту, `GET /upload/batch/<job_id>` — файлы, а SSE `GET /upload/events` сообщит об обновлениях. Через восемь часов после завершения воркер удалит записи из PostgreSQL. Готовые файлы останутся в Samba.

## Архив и API

Положи `data.json` в корень ZIP. Например, архив содержит `data.json`, `report.txt` и `docs/book.pdf`. В JSON указываются имена без папок и любые дополнительные поля.

```json
[
  {"filename": "report.txt", "author": "Иван"},
  {"filename": "book.pdf", "category": "Документы"}
]
```

Сопоставление точное, с учётом регистра. Структура папок сохраняется при записи. Для неоднозначного имени, дубликата в JSON, отсутствующего файла или отсутствующих метаданных появляется строка с `status: "error"` и причиной в `reason`. Архив с некорректным `data.json` целиком получает статус `failed`. Пределы по умолчанию составляют 256 МиБ для ZIP, 2 ГиБ для распакованных данных, 5000 записей в ZIP и 2 МиБ для `data.json`. Воркер читает содержимое файлов из ZIP по одному и не распаковывает весь архив на диск.

| Запрос | Результат |
|---|---|
| `POST /upload/batch` | Один ZIP в поле `archive`. Ответ содержит `job_id` и `status: "queued"`. |
| `GET /upload/batch?hours=8` | Активные задачи любого возраста и завершённые за последние восемь часов. |
| `GET /upload/batch/<job_id>` | Сводка, список файлов, метаданные, причины ошибок и ссылки на скачивание. |
| `GET /upload/events` | SSE-событие `sync` при подключении и после изменений. |
| `POST /upload/batch/<job_id>/cancel` | Отмена в очереди с ответом `200` или запрос на остановку после текущего файла с ответом `202`. |
| `GET /upload/files/<file_id>` | Скачивание файла из хранилища. |

```sh
curl -f -F archive=@batch.zip http://localhost:8000/upload/batch
curl -f 'http://localhost:8000/upload/batch?hours=8'
curl -N http://localhost:8000/upload/events
```

Задача проходит через `queued`, `validating` и `uploading`. Возможные финалы — `done`, `done_with_errors`, `failed`, `cancelled` и `interrupted`. Пока состав ZIP проверяется, `total` равен `null`. В `processed` входят загруженные, существующие и ошибочные файлы. Статус `cancelling` означает, что воркер закончит текущую запись, затем отменит оставшиеся файлы. Готовые файлы остаются доступны.

Если воркер аварийно остановился, при следующем запуске он помечает активную задачу как `interrupted` и продолжает очередь. Подтверждённые результаты сохраняются. Для файла, который в момент сбоя копировался, результат записи может быть неизвестен. При повторной отправке ZIP готовый объект по тому же ключу учитывается как `exists`.

Воркер проверяет срок хранения при старте и раз в минуту, даже пока идёт запись в хранилище. Через восемь часов после `finished_at` он удаляет завершённую задачу и связанные строки файлов из PostgreSQL. Срок задаётся через `BATCH_RETENTION_HOURS`, окно ленты — через `BATCH_HISTORY_HOURS`. Активные задачи и готовые файлы в хранилище сохраняются. После удаления записи запросы деталей и скачивание по старому `file_id` возвращают `404`.

Временные ZIP удаляются по завершении задачи или при восстановлении после сбоя. После жёсткой остановки могут остаться незавершённые временные файлы `.batch-part-*` на шаре или папка ZIP без записи в БД. Такие остатки можно удалять при остановленных `web` и `worker`, сверив папки ZIP с очередью.

## Фронтенд

Передавай ZIP через `FormData` в поле `archive`. Если нужен процент передачи ZIP, используй `XMLHttpRequest.upload.onprogress`. После ответа `202` можно показать новую строку или открыть задачу по `job_id`.

Событие `sync` сообщает об изменении. `GET /upload/batch` возвращает свежую ленту, а `GET /upload/batch/<job_id>` — свежие детали выбранного архива. Первое событие приходит сразу, в том числе после переподключения. Одно подключение обслуживает всю модалку. При закрытии вызови `close()`, при выборе задачи — `refresh()`. Если детали удалённой задачи вернули `404`, пример передаёт `null` в `renderJob`, чтобы закрыть панель деталей.

```js
function watchUploads(getSelectedJobId, renderJobs, renderJob, showError) {
  const cancel = new AbortController();
  const events = new EventSource("/upload/events");
  let pending = false;
  let running = false;

  async function get(url, allowMissing = false) {
    const response = await fetch(url, { signal: cancel.signal, cache: "no-store" });
    if (allowMissing && response.status === 404) return null;
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json();
  }

  async function refresh() {
    pending = true;
    if (running) return;
    running = true;
    while (pending && !cancel.signal.aborted) {
      pending = false;
      const id = getSelectedJobId();
      try {
        const [list, job] = await Promise.all([
          get("/upload/batch"),
          id ? get(`/upload/batch/${encodeURIComponent(id)}`, true) : null,
        ]);
        renderJobs(list.jobs);
        if (id && id === getSelectedJobId()) renderJob(job);
      } catch (error) {
        if (error.name !== "AbortError") showError(error);
      }
    }
    running = false;
  }

  events.addEventListener("sync", refresh);
  void refresh();
  return {
    refresh,
    close() {
      events.close();
      cancel.abort();
    },
  };
}
```

При подключении через прокси сохрани потоковую отдачу для `/upload/events` и выставь таймаут чтения выше 15 секунд. Лимит размера запроса у прокси должен учитывать максимальный размер ZIP и multipart-обёртку. Если Flask уже защищён аутентификацией, используй действующие правила доступа и для новых маршрутов, включая SSE и скачивание.
