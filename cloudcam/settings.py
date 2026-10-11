"""Настройки — библиотека без config (через объект/параметры).

Раньше использовался `import config` (глобальный модуль) — библиотека так
не может. Теперь все настройки в объекте `Settings`. Три источника, в порядке
приоритета:

    1. Прямо переданные значения  (Settings(email=..., password=...))
    2. Переменные окружения          (CLOUDCAM_EMAIL, ...)
    3. Старый модуль `config.py`      (если существует — для обратной совместимости)
    4. Значения по умолчанию

`set_active()` / `get_active()` — активная настройка на уровне модуля (proxy
и stream_manager вместо глобального config читают именно её).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, fields

ENV_PREFIX = "CLOUDCAM_"
_MISSING = object()
_TRUE = ("1", "true", "yes", "on")

# Поля, НЕ передаваемые дочернему процессу. Пароль в переменной окружения
# виден через список процессов во многих системах, а дочернему процессу
# он вообще не нужен — он читает токен из файла. `isup_token` — то же самое:
# только родительский процесс общается с мостом.
_ENV_EXCLUDE = frozenset({"password", "isup_token"})


def _coerce(type_name, raw: str):
    """Приводит строку к типу поля (из-за `from __future__ import annotations`
    `field.type` — это строка, например "str | None")."""
    t = str(type_name)
    if "bool" in t:
        return raw.strip().lower() in _TRUE
    if "int" in t:
        return int(raw)
    return raw


@dataclass
class Settings:
    platform: str = "hikconnect"          # "hikconnect" | "ezviz"
    email: str = ""
    password: str = ""
    region: str | None = None             # None — определяется при входе (login)
    proxy_start_port: int = 8700          # последовательные порты, по одному на камеру
    display_width: int = 1280
    display_height: int = 720
    use_gpu: bool = True                  # NVDEC; если не работает — программное
    # Запрашивать ли из облака мелкий поток (substream). Для сетки (grid) очень полезно:
    # замер — плитка 640x360 / ~0.11 Mbit/s, а основной поток ~4 Mbit/s
    # (в 36 раз меньше трафика и CPU). Для полного экрана оставьте False.
    substream: bool = False
    # Какой поток запрашивать: "main" | "sub" | "auto".
    # "auto" — сначала запрашивается substream, если не пришёл — откат на основной
    # ([[decrypt_proxy.serve]]). Substream есть не на всех камерах (например,
    # батарейные модели), поэтому жёсткое "sub" — рискованно.
    stream_mode: str = "main"
    token_file: str = "token.json"
    camkey_file: str = "cam_keys.json"
    # ── ISUP 5.0 ([[isup]]) ───────────────────────────────────────────
    # Устройство подключается не к облаку, а к `isup-bridge`. Мост — отдельный
    # процесс (внешняя зависимость, как ffmpeg) — здесь только его
    # адрес. Без моста ничего не ломается: облачный путь
    # продолжает работать как прежде.
    isup_url: str = "http://127.0.0.1:8091"      # HTTP API моста
    isup_token: str = ""                          # X-Bridge-Token (необязательно)
    isup_rtsp_base: str = "rtsp://127.0.0.1:8554"  # MediaMTX, куда мост публикует

    # ── источники ──────────────────────────────────────────────────────
    @classmethod
    def from_config_module(cls) -> "Settings":
        """Из старого `config.py` (если существует) — обратная совместимость.

        Имена полей приводятся к UPPER_CASE, поэтому при добавлении нового
        поля не ОБЯЗАТЕЛЬНО редактировать это место (раньше `substream`
        не читался из config именно по этой причине)."""
        try:
            import config as c
        except Exception:
            return cls()
        vals = {}
        for f in fields(cls):
            v = getattr(c, f.name.upper(), _MISSING)
            if v is not _MISSING:
                vals[f.name] = v
        return cls(**vals)

    @classmethod
    def from_env(cls, base: "Settings | None" = None) -> "Settings":
        """Наложить переменные окружения `CLOUDCAM_*` поверх `base`.

        Поля обрабатываются автоматически: `substream` -> `CLOUDCAM_SUBSTREAM`."""
        s = base or cls()
        vals = {}
        for f in fields(cls):
            raw = os.environ.get(f"{ENV_PREFIX}{f.name.upper()}")
            if raw is None:
                continue
            vals[f.name] = _coerce(f.type, raw)
        return s.replace(**vals)

    def to_env(self) -> dict[str, str]:
        """Преобразует настройки в пары `CLOUDCAM_*`.

        Зачем: каждая камера — ОТДЕЛЬНЫЙ ПРОЦЕСС ([[stream_manager]]), и
        процесс получает настройки только из окружения. Раньше передавалось
        только `token_file` — в итоге в режиме библиотеки поток, открытый
        как `CloudCam(platform="ezviz")`, в дочернем процессе принимался
        как `hikconnect`, отправлялся неверный `clientType` и pagelist приходил не полностью."""
        out = {}
        for f in fields(self):
            if f.name in _ENV_EXCLUDE:
                continue
            v = getattr(self, f.name)
            if v is None:
                continue
            out[f"{ENV_PREFIX}{f.name.upper()}"] = (
                ("1" if v else "0") if isinstance(v, bool) else str(v))
        return out

    @classmethod
    def resolve(cls) -> "Settings":
        """Стандартное решение: config.py (если есть) -> переменные окружения."""
        return cls.from_env(cls.from_config_module())

    def replace(self, **kw) -> "Settings":
        data = {f.name: getattr(self, f.name) for f in fields(self)}
        data.update(kw)
        return Settings(**data)


# ── активные настройки на уровне модуля ──────────────────────────────────
# stream_manager и decrypt_proxy читают именно их. Default — config.py (если
# существует) и переменные окружения автоматически, т.е. сохраняется старое
# поведение `import config`.
_active: Settings | None = None


def get_active() -> Settings:
    global _active
    if _active is None:
        _active = Settings.resolve()
    return _active


def set_active(settings: Settings) -> None:
    global _active
    _active = settings