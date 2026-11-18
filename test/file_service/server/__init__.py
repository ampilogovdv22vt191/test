from flask import Flask

from batch_upload import init_batch


# В действующем сервисе этот объект и его настройки уже есть.
app = Flask(__name__)
init_batch(app)
