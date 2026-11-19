import ctypes
import errno
import os
from pathlib import Path
import shutil
import sys
from tempfile import NamedTemporaryFile
from typing import BinaryIO, Literal, Protocol


CHUNK_SIZE = 1024 * 1024
WriteResult = Literal["uploaded", "exists"]


class Storage(Protocol):
    """Интерфейс, через который воркер пишет объекты, а Flask отдаёт их на чтение."""

    def put_if_absent(self, key: str, source: BinaryIO, *, size: int) -> WriteResult:
        """Пишет поток с начала файла и публикует объект без перезаписи.

        Возвращает uploaded после полной записи или exists, когда готовый
        объект по этому же key можно открыть через open_read. API использует
        этот ключ при скачивании. Поток source открывает и закрывает воркер.
        """
        ...

    def open_read(self, key: str) -> BinaryIO:
        """Открывает объект по ключу. При отсутствии поднимает FileNotFoundError.

        Поток закрывает вызывающий код. Flask использует FileNotFoundError
        для ответа 404 при скачивании.
        """
        ...


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


def rename_without_replace(source: Path, target: Path) -> None:
    """Атомарно публикует временный файл под целевым именем.

    Вызывает системную операцию Linux или macOS, которая сохраняет уже
    существующий целевой файл. При конфликте или другом сбое поднимает
    OSError с кодом системы, чтобы адаптер мог обработать результат.
    """

    # На Linux, в том числе для CIFS, используется RENAME_NOREPLACE.
    # На macOS ту же гарантию даёт RENAME_EXCL.
    libc = ctypes.CDLL(None, use_errno=True)
    old, new = os.fsencode(source), os.fsencode(target)
    if sys.platform == "linux":
        rename = libc.renameat2
        rename.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
        result = rename(-100, old, -100, new, 1)
    elif sys.platform == "darwin":
        rename = libc.renamex_np
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        result = rename(old, new, 4)
    else:
        raise OSError(errno.ENOTSUP, "Atomic publication requires Linux or macOS")
    if result:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), str(target))


class MountedStorage:
    """Хранилище поверх локального каталога или примонтированной SMB-шары."""

    def __init__(self, root: str | Path):
        """Сохраняет абсолютный путь к существующей директории хранилища."""

        self.root = Path(root).resolve(strict=True)
        if not self.root.is_dir():
            raise NotADirectoryError(self.root)

    def _path(self, key: str) -> Path:
        """Преобразует ключ в путь под root и проверяет компоненты на симлинки."""

        target = self.root
        for part in key_parts(key):
            target = target / part
            # Проверяем каждый компонент, чтобы путь не ушёл за пределы шары по ссылке.
            if target.is_symlink():
                raise OSError(errno.ELOOP, "Symbolic links are disabled in batch storage")
        return target

    def put_if_absent(self, key: str, source: BinaryIO, *, size: int) -> WriteResult:
        """Записывает входной поток в каталог с сохранением структуры ключа.

        Если обычный файл уже есть, возвращает exists. Иначе пишет во
        временный файл, сверяет размер и переименовывает без перезаписи.
        Возвращает uploaded после публикации и убирает временную копию
        при конфликте или ошибке.
        """

        target = self._path(key)
        if target.is_file():
            return "exists"
        if target.exists():
            raise IsADirectoryError(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            # Временный и целевой файлы находятся в одном каталоге.
            # Конечное имя появится после записи всех байтов.
            with NamedTemporaryFile(dir=target.parent, prefix=".batch-part-", delete=False) as out:
                temporary = Path(out.name)
                shutil.copyfileobj(source, out, length=CHUNK_SIZE)
                if out.tell() != size:
                    raise OSError("Размер записанного файла не совпадает с указанным в ZIP")
            try:
                rename_without_replace(temporary, target)
            except FileExistsError:
                # Файл мог появиться, пока мы записывали временную копию.
                if target.is_file() and not target.is_symlink():
                    return "exists"
                raise
            return "uploaded"
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def open_read(self, key: str) -> BinaryIO:
        """Открывает обычный файл в двоичном режиме для потокового скачивания."""

        path = self._path(key)
        if not path.is_file():
            raise FileNotFoundError(path)
        return path.open("rb")
