import errno
import logging
import ntpath
import shutil
import stat
from typing import BinaryIO
from uuid import uuid4

import smbclient
from smbclient.path import islink
from smbprotocol.exceptions import SMBException

from .storage import CHUNK_SIZE, WriteResult, key_parts


log = logging.getLogger(__name__)


class SmbStorage:
    """Читает и записывает файлы на удалённой Samba через SMB2/SMB3."""

    def __init__(self, root: str, username: str, password: str):
        root = root.replace("/", "\\").rstrip("\\")
        if not root.startswith("\\\\") or len(root[2:].split("\\")) < 2:
            raise ValueError("BATCH_SMB_ROOT должен иметь вид //сервер/шара или //сервер/шара/папка")
        key_parts(root[2:].replace("\\", "/"))
        self.root = root
        # smbclient открывает и переиспользует соединение при первой операции с файлом.
        self.options = {"username": username, "password": password, "connection_timeout": 15}

    def _path(self, key: str) -> str:
        *parents, filename = key_parts(key)
        path = self.root
        for parent in parents:
            path = ntpath.join(path, parent)
            if islink(path, **self.options):
                raise OSError(errno.ELOOP, "Символические ссылки в ключах SMB запрещены")
        return ntpath.join(path, filename)

    def _existing_file(self, path: str) -> bool:
        """Проверяет наличие обычного файла по пути SMB.

        False означает, что пути нет. Ошибка доступа или соединения должна
        дойти до воркера, чтобы у файла появилась причина сбоя.
        """

        try:
            info = smbclient.lstat(path, **self.options)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                return False
            raise
        if not stat.S_ISREG(info.st_mode):
            raise OSError(errno.EINVAL, "По целевому пути SMB находится не обычный файл")
        return True

    def put_if_absent(self, key: str, source: BinaryIO, *, size: int) -> WriteResult:
        target = self._path(key)
        if self._existing_file(target):
            # Сейчас признак дубля — существование файла по тому же ключу.
            # Для сравнения содержимого по этому ключу добавь проверку здесь.
            # Если пропускаешь только при совпадении хешей, учти также ветку
            # EEXIST после rename, где другой процесс мог записать файл первым.
            return "exists"
        parent = ntpath.dirname(target)
        if parent.rstrip("\\") != self.root:
            smbclient.makedirs(parent, exist_ok=True, **self.options)
        temporary = ntpath.join(parent, f".batch-part-{uuid4().hex}")
        created = False
        try:
            with smbclient.open_file(temporary, mode="xb", **self.options) as out:
                created = True
                # Если нужен хеш новой копии, его можно считать в цикле записи
                # порциями. Сверь результат до публикации конечного имени.
                shutil.copyfileobj(source, out, length=CHUNK_SIZE)
                if out.tell() != size:
                    raise OSError("Размер записанного файла не совпадает с указанным в ZIP")
            try:
                # Между первой проверкой и публикацией другой загрузчик может
                # занять целевое имя. Оставь rename без перезаписи, даже если
                # добавишь свою проверку дубликатов перед записью.
                smbclient.rename(temporary, target, **self.options)
            except OSError as exc:
                if exc.errno == errno.EEXIST and self._existing_file(target):
                    return "exists"
                raise
            created = False
            return "uploaded"
        finally:
            if created:
                try:
                    smbclient.remove(temporary, **self.options)
                except (OSError, SMBException) as exc:
                    if getattr(exc, "errno", None) != errno.ENOENT:
                        log.warning("Не удалось удалить временный файл Samba %s", temporary, exc_info=True)

    def open_read(self, key: str) -> BinaryIO:
        try:
            target = self._path(key)
            if not self._existing_file(target):
                raise FileNotFoundError(errno.ENOENT, "Файл отсутствует в Samba", target)
            return smbclient.open_file(target, mode="rb", **self.options)
        except FileNotFoundError:
            raise
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                raise FileNotFoundError(errno.ENOENT, "Файл отсутствует в Samba") from exc
            raise
        except SMBException as exc:
            raise OSError("Не удалось открыть файл через SMB") from exc
