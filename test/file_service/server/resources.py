from flask_restful import Resource


class Health(Resource):
    """Проверка HTTP-сервиса и пример регистрации ресурса через Api."""

    def get(self):
        """Возвращает JSON с признаком, что веб-сервис отвечает на GET /health."""

        return {"status": "ok"}
