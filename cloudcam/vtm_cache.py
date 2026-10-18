"""Файловый кеш облачных метаданных для УЧЁТНОЙ ЗАПИСИ.

Проблема: `open_cloud_stream` при каждом открытии делает два облачных запроса —
`get_vtm_page_list` (многостраничный при многих NVR на аккаунте, замер ~4.3s / 236
ресурсов) и `get_vtdu_token_v2` (~1.8s). Оба относятся к АККАУНТУ, а не к КАМЕРЕ:
ответ один и тот же для 32 камер одного аккаунта. Без кеша каждая камера
при каждом повторном подключении зря платит эти ~6s и бьёт по облаку.

Замер (одна камера, одинаковые условия): тёплый кеш **0.24s**, без кеша **5.76s**.

Каждая камера — отдельный ПРОЦЕСС, поэтому кеш не в памяти, а в ФАЙЛЕ — так он
работает и между процессами.

ВАЖНО: кеш инвалидируется только когда поток ВООБЩЕ не открыт. Опыт прошлого:
"инвалидировать при каждом обрыве" — из-за секундного обрыва на одной камере
сносился кеш ВСЕХ камер В ЭТОМ АККАУНТЕ, и следующие открытия заново
грузили полные метаданные. Если хотя бы один пакет дошёл — кеш был верен.
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from typing import Any

from .settings import get_active

TTL_SECONDS = 3600.0            # срок действия кеша
_ENV_TTL = "CLOUDCAM_VTM_TTL"


def _ttl() -> float:
    try:
        return float(os.environ.get(_ENV_TTL) or TTL_SECONDS)
    except ValueError:
        return TTL_SECONDS


def cache_dir() -> str:
    """Место файлов кеша — рядом с файлом токена (он тоже привязан к аккаунту)."""
    tok = get_active().token_file or "token.json"
    d = os.path.dirname(os.path.abspath(tok))
    return d or tempfile.gettempdir()


def account_key(token_file: str | None = None) -> str:
    """Ключ аккаунта — из имени файла токена (`token.json` -> `token`)."""
    tok = token_file or get_active().token_file or "token.json"
    return os.path.splitext(os.path.basename(tok))[0] or "default"


def _path(key: str, kind: str) -> str:
    safe = "".join(c for c in key if c.isalnum() or c in "-_")
    return os.path.join(cache_dir(), f"vtm-{safe}-{kind}.json")


def _is_error(data: Any) -> bool:
    """Ответ облака с ошибкой (meta.code != 200).

    Такие ответы НЕ кешируем: облако периодически отваливается
    (504 «Rpc invoke exception»), и закэшированная ошибка затем
    подменяет живой ответ на весь TTL."""
    if not isinstance(data, dict):
        return False
    meta = data.get("meta")
    if isinstance(meta, dict) and meta.get("code"):
        return meta.get("code") != 200
    return False


def load(key: str, kind: str) -> Any | None:
    """Значение, если кеш свежий, иначе None."""
    try:
        with open(_path(key, kind), encoding="utf-8") as f:
            blob = json.load(f)
        if time.time() - float(blob.get("ts") or 0) > _ttl():
            return None
        return blob.get("data")
    except (OSError, ValueError, TypeError):
        return None


def store(key: str, kind: str, data: Any) -> None:
    """Пишет АТОМАРНО — несколько процессов могут писать одновременно."""
    try:
        os.makedirs(cache_dir(), exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=cache_dir(), suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"ts": time.time(), "data": data}, f)
        os.replace(tmp, _path(key, kind))
    except (OSError, TypeError, ValueError):
        pass                       # кеш — оптимизация, ошибка не должна останавливать работу


def invalidate(key: str, kind: str | None = None) -> None:
    """Очищает кеш. Вызывайте ТОЛЬКО когда поток не открыт."""
    for k in ((kind,) if kind else ("pagelist", "vtdu")):
        try:
            os.remove(_path(key, k))
        except OSError:
            pass


def age(key: str, kind: str) -> float | None:
    try:
        with open(_path(key, kind), encoding="utf-8") as f:
            blob = json.load(f)
        return time.time() - float(blob.get("ts") or 0)
    except (OSError, ValueError, TypeError):
        return None


# ---- Шлюз по каналам ------------------------------------------------------

def available_channels(key: str) -> dict[str, set[int]] | None:
    """{serial: {доступные каналы}} из закэшированного pagelist.

    Даже если NVR "online" в облаке, НЕ СУЩЕСТВУЮЩЕГО канала поток
    никогда не даст — такая камера выглядит "online, но не открывается".
    У нас 2 камеры были настроены на несуществующие каналы NVR."""
    data = load(key, "pagelist")
    if data is None:
        return None
    out: dict[str, set[int]] = {}

    def rec(x: Any) -> None:
        if isinstance(x, dict):
            ser = x.get("deviceSerial") or x.get("serial")
            ch = x.get("channelNo", x.get("localIndex", x.get("channel")))
            if isinstance(ser, str):
                out.setdefault(ser, set())
                if ch is not None:
                    try:
                        out[ser].add(int(ch))
                    except (TypeError, ValueError):
                        pass
            for v in x.values():
                rec(v)
        elif isinstance(x, list):
            for i in x:
                rec(i)

    rec(data)
    return out


def channel_missing(key: str, serial: str, channel: int) -> bool:
    """Отсутствует ли (serial, канал) в облачных ресурсах?

    В неопределённых случаях ВСЕГДА False — не знать лучше, чем
    сказать неверное "нет": кеша нет, serial нет, или множество каналов ПУСТОЕ
    (одноканальное устройство) — в этом случае ничего не утверждаем."""
    chans = available_channels(key)
    if not chans:
        return False
    have = chans.get(serial)
    if not have:
        return False
    real = {c for c in have if c != 0}      # 0 — часто "само устройство"
    if not real:
        return False
    return channel not in have


# ---- обёртка pyezvizapi кешом --------------------------------------

def install(token_file: str | None = None) -> str:
    """Наложить кеш на `get_vtm_page_list` и `get_vtdu_token_v2`.

    Патч накладывается НАД патчем пагинации — если кеш пуст, всё равно
    берётся ПОЛНЫЙ (многостраничный) список. Возвращает: ключ аккаунта (для invalidate).
    """
    import pyezvizapi.cloud_stream as _cs

    key = account_key(token_file)
    if getattr(_cs, "_cloudcam_cache_key", None) == key:
        return key                            # уже установлен
    inner_pagelist = _cs.get_vtm_page_list
    inner_vtdu = _cs.get_vtdu_token_v2

    def _cached_pagelist(client):
        hit = load(key, "pagelist")
        if hit is not None:
            if _is_error(hit):
                hit = None        # в кеше застыла ошибка облака — переспрашиваем
            else:
                return hit
        data = inner_pagelist(client)
        if not _is_error(data):   # ошибки облака в кеш не пишем
            store(key, "pagelist", data)
        return data

    def _cached_vtdu(client, *a, **kw):
        hit = load(key, "vtdu")
        if hit is not None:
            if _is_error(hit):
                hit = None
            else:
                return hit
        data = inner_vtdu(client, *a, **kw)
        if not _is_error(data):
            store(key, "vtdu", data)
        return data

    _cs.get_vtm_page_list = _cached_pagelist
    _cs.get_vtdu_token_v2 = _cached_vtdu
    _cs._cloudcam_cache_key = key
    return key