import os

from flask import Flask
from flask_cors import CORS
from flask_restful import Api
from werkzeug.utils import import_string

from batch_upload import init_batch
from .resources import Health


# В действующем сервисе этот объект и его настройки уже есть.
app = Flask(__name__)
CORS(app)
api = Api(app)
api.add_resource(Health, "/health")

# В действующем сервисе оставь здесь регистрацию своих Resource.
save_file_path = os.environ.get("BATCH_SAVE_FILE")
if not save_file_path:
    raise RuntimeError("Укажи BATCH_SAVE_FILE с импортируемой функцией записи в Samba")
file_exists_path = os.environ.get("BATCH_FILE_EXISTS_ERROR")
# Функция вызывается с метаданными файла и результатом записи в Samba.
after_save_path = os.environ.get("BATCH_AFTER_SAVE")
init_batch(
    app,
    save_file=import_string(save_file_path),
    file_exists_error=import_string(file_exists_path) if file_exists_path else FileExistsError,
    after_save=import_string(after_save_path) if after_save_path else None,
)
