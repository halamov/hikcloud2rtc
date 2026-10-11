"""Идентификатор терминала (устройства) — `featureCode`.

Облако узнаёт каждый «терминал» по этому коду: лимит входа, CAPTCHA
и 2FA-проверка «новое устройство» — всё завязано на него.

Раньше здесь был ЗАжёстко записанный единственный код
(`1fc28fa018178a1cd1c091b13b2f9f02`) — скопированный в трёх файлах. Это вредно
с обеих сторон:

  1. Каждая копия проекта облаку видна как ОДИН терминал.
     Облако при этом ограничивает число аккаунтов, заходящих с одного терминала
     (у EZVIZ это «terminal limit», code 1069), а при подозрительной активности
     требует CAPTCHA (1015) — то есть чужая активность может ударить по вашему аккаунту.
  2. Даже если код при каждом запуске одинаков, смена аккаунта облаку
     видится как «много аккаунтов на одном устройстве».

Решение (по которому идут `pyezvizapi` и `ezviz-ha-addon`): код должен
быть привязан к ЭТОЙ УСТАНОВКЕ. Берётся из MAC-адреса — то есть на одном
компьютере ВСЕГДА один и тот же (облако узнаёт «то же устройство» и не
запрашивает 2FA повторно), но на другом компьютере другой.

MAC иногда меняется (VPN/виртуальный адаптер, dock-станция). Поэтому
расчётный код ещё и пишется в файл, а в следующий раз читается ИЗ ЭТОГО файла:
даже при смене адаптера терминал не меняется.
"""
from __future__ import annotations

import hashlib
import os
import uuid

_FILENAME = "feature_code.txt"
_ENV = "CLOUDCAM_FEATURE_CODE"
_cached: str | None = None


def _from_mac() -> str:
    """Стабильный 32-символьный код из MAC-адреса (тот же метод, что в pyezvizapi)."""
    mac_int = uuid.getnode()
    mac_str = ":".join(f"{(mac_int >> i) & 0xFF:02x}" for i in range(40, -1, -8))
    return hashlib.md5(mac_str.encode("utf-8")).hexdigest()


def _store_path() -> str:
    """Файл, где хранится код — рядом с файлом token (он тоже привязан к этой установке)."""
    from .settings import get_active
    tok = get_active().token_file or "token.json"
    d = os.path.dirname(os.path.abspath(tok))
    return os.path.join(d, _FILENAME)


def feature_code() -> str:
    """Терминальный код этой установки (32 hex-символа).

    Приоритет: `CLOUDCAM_FEATURE_CODE` -> сохранённый файл -> расчёт из MAC.
    Рассчитанный код первый раз пишется в файл.
    """
    global _cached
    if _cached:
        return _cached
    env = (os.environ.get(_ENV) or "").strip()
    if env:
        _cached = env
        return _cached
    path = _store_path()
    try:
        with open(path, encoding="utf-8") as f:
            saved = f.read().strip()
        if saved:
            _cached = saved
            return _cached
    except OSError:
        pass
    code = _from_mac()
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(code)
    except OSError:
        pass            # даже если не сможем записать — всё равно будет работать, MAC стабилен
    _cached = code
    return _cached


def reset_cache() -> None:
    """Для тестов — сброс кешированного кода."""
    global _cached
    _cached = None
