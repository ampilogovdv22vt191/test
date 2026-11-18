from collections import Counter, defaultdict
from dataclasses import dataclass
import json
from zipfile import ZipFile, ZipInfo

from .storage import key_parts


class ArchiveError(ValueError):
    pass


@dataclass(frozen=True)
class Entry:
    """Результат сопоставления имени из data.json с содержимым ZIP.

    Запись с error видна в деталях загрузки сразу как ошибка. Воркер возьмёт
    на запись только записи, у которых найден файл и пройдены проверки.
    """

    filename: str
    member: ZipInfo | None = None
    meta: dict | None = None
    error: str | None = None


def _safe_parts(name: str) -> list[str]:
    """Проверяет, что путь можно сохранить в PostgreSQL и использовать как ключ."""

    try:
        name.encode("utf-8")
        return key_parts(name)
    except (ValueError, UnicodeError):
        raise ArchiveError("ZIP или data.json содержит недопустимый путь") from None


def _manifest(zip_file: ZipFile, info: ZipInfo, limit: int) -> list[dict]:
    if info.file_size > limit:
        raise ArchiveError("data.json превышает допустимый размер")
    try:
        with zip_file.open(info) as source:
            raw = source.read(limit + 1)
    except (NotImplementedError, RuntimeError) as exc:
        raise ArchiveError("Не удалось прочитать data.json из ZIP") from exc
    if len(raw) > limit:
        raise ArchiveError("data.json превышает допустимый размер")
    try:
        result = json.loads(raw.decode("utf-8-sig"))
        # Стандартный декодер принимает NaN и Infinity. PostgreSQL отклоняет их.
        json.dumps(result, allow_nan=False)
    except (ValueError, RecursionError):
        raise ArchiveError("data.json должен содержать корректный JSON в UTF-8") from None
    # Здесь проверяется контракт всего data.json. ArchiveError завершит задачу
    # целиком. Правила, применимые к одному файлу, добавляй в inspect_zip.
    if not isinstance(result, list):
        raise ArchiveError("data.json должен содержать массив объектов")
    for item in result:
        if not isinstance(item, dict) or not isinstance(item.get("filename"), str):
            raise ArchiveError("Каждой записи data.json требуется строковое поле filename")
        if len(_safe_parts(item["filename"])) != 1:
            raise ArchiveError("В filename указывается имя файла без пути")
    return result


def inspect_zip(
    zip_file: ZipFile,
    *,
    max_members: int,
    max_unpacked: int,
    max_manifest: int,
) -> list[Entry]:
    """Проверяет архив и сопоставляет метаданные с файлами без полной распаковки."""

    members = zip_file.infolist()
    if len(members) > max_members:
        raise ArchiveError("В ZIP слишком много записей")
    if sum(member.file_size for member in members) > max_unpacked:
        raise ArchiveError("Содержимое ZIP превышает допустимый размер")
    files_by_name: dict[str, list[ZipInfo]] = defaultdict(list)
    manifest = None
    paths = set()
    for member in members:
        # Проверяем исходное имя, даже если ZipFile нормализовал его.
        name = member.orig_filename.removesuffix("/") if member.is_dir() else member.orig_filename
        parts = _safe_parts(name)
        if name in paths:
            raise ArchiveError("ZIP содержит повторяющийся путь")
        paths.add(name)
        if member.flag_bits & 1:
            raise ArchiveError("ZIP с паролем не поддерживается")
        if member.create_system == 3:
            # Для записей, созданных в Unix, тип файла хранится в старших битах.
            kind = (member.external_attr >> 16) & 0o170000
            if kind not in (0, 0o100000, 0o040000):
                raise ArchiveError("ZIP содержит ссылку или специальный файл")
        if member.is_dir():
            continue
        if name == "data.json":
            manifest = member
        else:
            files_by_name[parts[-1]].append(member)
    if manifest is None:
        raise ArchiveError("В корне ZIP отсутствует data.json")

    metadata = _manifest(zip_file, manifest, max_manifest)
    if len(metadata) > max_members:
        raise ArchiveError("В data.json слишком много записей")
    names = Counter(item["filename"] for item in metadata)
    result = []
    # Сначала идём в порядке data.json. Полный путь сохраняем при точном совпадении.
    for item in metadata:
        name = item["filename"]
        matching = files_by_name.get(name, [])
        if names[name] > 1:
            result.append(Entry(name, meta=item, error="duplicate_metadata"))
        elif not matching:
            result.append(Entry(name, meta=item, error="file_not_found_in_archive"))
        elif len(matching) > 1:
            result.append(Entry(name, meta=item, error="ambiguous_filename"))
        else:
            # Здесь уже есть одна запись JSON и ровно один файл в ZIP. Место
            # для проверок расширения, размера из ZipInfo или полей item.
            # Для отказа одного файла сохрани Entry с error и своей причиной.
            # Проверки содержимого и запросы к хранилищу выполняй в Worker.process.
            result.append(Entry(name, member=matching[0], meta=item))
    # Файлы без метаданных также видны в деталях с отдельной причиной ошибки.
    for name, matching in files_by_name.items():
        if name not in names:
            for member in matching:
                result.append(Entry(name, member=member, error="no_metadata"))
    if not result:
        raise ArchiveError("В ZIP нет файлов для загрузки")
    return result
