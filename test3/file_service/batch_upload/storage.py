from collections.abc import Callable
from io import BytesIO


CHUNK_SIZE = 1024 * 1024
SaveFile = Callable[[BytesIO], object]


def key_parts(key: str) -> list[str]:
    """Разбивает ключ вида каталог/файл на безопасные части пути.

    Проверяет каждый компонент. Запрещает абсолютные пути, переходы
    через .., служебный префикс временных файлов и символы, недопустимые
    для этого адаптера. При нарушении поднимает ValueError.
    """

    parts = key.split("/")
    if any(
        part in ("", ".", "..")
        or part.endswith((".", " "))
        or part.startswith(".batch-part-")
        or any(char in '\\:*?"<>|' or ord(char) < 32 for char in part)
        for part in parts
    ):
        raise ValueError("Недопустимый путь к файлу")
    return parts
