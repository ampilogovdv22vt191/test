from flask import Flask
from flask_cors import CORS
from flask_restful import Api

from batch_upload import init_batch
from .resources import Health


# В действующем сервисе этот объект и его настройки уже есть.
app = Flask(__name__)
CORS(app)
api = Api(app)
api.add_resource(Health, "/health")

# В действующем сервисе оставь здесь регистрацию своих Resource.
init_batch(app)
