"""
Декодирующий прокси для зашифрованных EZVIZ HEVC камер.

Эти камеры отправляют видео в виде RTP/HEVC (H.265) и шифруют NAL body
с AES-ECB. Ключ = "код подтверждения" (verification code) камеры,
дополненный нулями до 16 байт. Первые до 4096 байт каждого NAL body
(полные 16-байтовые блоки) зашифрованы.

Стандартный "stream proxy" pyezvizapi не может это открыть (он ждёт MPEG-PS).
Здесь мы делаем это сами:
  1) читаем пакеты из VTM (RTP, MPEG-PS или MPEG-TS — определяется автоматически)
  2) разупаковываем RTP/HEVC и RTP/H.264 (VPS/SPS/PPS + FU/AP) -> Annex-B
  3) декодируем каждый NAL body с помощью verification code
  4) передаём Annex-B напрямую по HTTP — без встроенного ffmpeg.
     Выход сглаживается через [[PacedWriter]]; codec определяет внешний ffmpeg
     (stream_manager) сам.

Запуск (обычно вызывает app.py/stream_manager):
    python -m cloudcam.decrypt_proxy <SERIAL> <PORT> <CODE> [CHANNEL] [--stream=auto]
"""

import json
import os
import subprocess
import sys
import tempfile
import time
import threading
from itertools import chain
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from Crypto.Cipher import AES

import requests as _rq
import pyezvizapi.cloud_stream as _cs
from pyezvizapi.client import EzvizClient
from pyezvizapi.cloud_stream import open_cloud_stream
from pyezvizapi.stream import rtp_payload

from . import vtm_cache
from .identity import feature_code
from .settings import get_active

# Тип клиента в зависимости от платформы (важно, чтобы pagelist вернул полный результат)
_CLIENT_TYPE = "55" if get_active().platform == "hikconnect" else "1"


def _paged_vtm_page_list(client):
    """Собирает VTM pagelist ПОЛНОСТЬЮ (все страницы).
    Библиотека берёт только 1-ю страницу и clientType у неё другой -> в аккаунтах
    со многими NVR камеры не находятся. Поэтому сырой запрос с правильным header."""
    tok = getattr(client, "_token", {}) or {}
    api = tok.get("api_url")
    sess = _rq.Session()
    sess.headers.update({
        "clientType": _CLIENT_TYPE, "lang": "en-US",
        "featureCode": feature_code(),        # специфичен для этой установки ([[identity]])
        "sessionId": str(tok.get("session_id")),
    })
    base = None
    merged_res, merged_vtm = [], {}
    offset = 0
    for _ in range(40):
        r = sess.get(f"https://{api}/v3/userdevices/v1/resources/pagelist",
                     params={"filter": "VTM", "groupId": -1, "limit": 50, "offset": offset},
                     timeout=25)
        pl = r.json()
        if base is None:
            base = pl
        res = pl.get("resourceInfos") or []
        merged_res.extend(res)
        vtm = pl.get("VTM")
        if isinstance(vtm, dict):
            merged_vtm.update(vtm)
        page = pl.get("page") or {}
        if not res or not page.get("hasNext"):
            break
        offset += len(res)
    if base is not None:
        base["resourceInfos"] = merged_res
        base["VTM"] = merged_vtm
    return base


# заменяем одностраничную функцию библиотеки полным пейджером
_cs.get_vtm_page_list = _paged_vtm_page_list
# ...и ПОВЕРХ НЕЁ ставим файл-кеш по аккаунту ([[vtm_cache]]):
# pagelist + VTDU token каждый раз при открытии занимали ~6s в облаке, с кешем ~0.2s.
_CACHE_KEY = vtm_cache.install()

HEVC_VIDEO_PT = 96                  # RTP payload type — video
NAL_ENCRYPTED_PREFIX = 4096         # зашифрованы первые ровно столько байт каждого NAL
START = b"\x00\x00\x00\x01"

# --- АВТООПРЕДЕЛЕНИЕ шифрования inter (P/B) slice ---
# "Селективный шифр" (шифруются только IRAP + param-set, P/B чистые) — в НЕКОТОРЫХ
# камерах это верно, в других НЕВЕРНО. При неверном предположении КАЖДЫЙ P-фрейм
# повреждается: изображение восстанавливается на I-фрейме, а между ними рассыпается —
# именно жалоба "на телефоне плавно, а здесь — битое". Измерение (40с потока, одна камера):
# 246 ошибок HEVC-декодера при предположении, 5 при полном расшифровании.
_INTER_SAMPLES = 8              # столько образцов inter NAL для решения
_INTER_MARGIN = 0.25            # "deshifr" выбирается только если заметно более структурирован
_INTER_MAX_PENDING = 4 << 20    # если inter NAL не приходит, сдаёмся после такого числа байт

# --- IRAP-шлюз ---
# Облачный поток начинается ПОСЕРЕДИНЕ GOP, и Hik NVR периодически
# отправляет param-set, то есть сразу после первого VPS/SPS/PPS IDR не приходит. Если
# передавать отсюда, декодер не находит кадры-ссылки, и зритель в начале видит СЕРЫЙ
# ЦВЕТ/повреждённое изображение. Поэтому до первого IRAP (IDR/CRA)
# НЕ выдаём slice; param-set проходят (нужны декодеру).
# После такого количества slice шлюз ОТКРЫВАЕТСЯ ПРИНУДИТЕЛЬНО — чтобы камеры, не
# отмечающие IRAP, не остались вообще без изображения.
_IRAP_MAX_WAIT = 400

# Верхний предел буферов MPEG-PS demux. При входе без start code/с повреждением
# буфер рос бы бесконечно (память процесса утекала) — при превышении предела
# делаем resync на последний start code.
_MAX_PS_BUF = 4 << 20


# ---- КЕШ решения об inter-шифре -------------------------------------------
# Зашифрованы ли inter (P/B) slice — это НЕИЗМЕННОЕ свойство КАМЕРЫ, но раньше
# определялось заново при каждом открытии, и до решения NAL держались в буфере.
# Измерение (тот же декодер на central-server): от "первого пакета"
# до "первого кадра в ffmpeg" 0.65-0.85s — именно это ожидание. С кешем
# первый кадр уходит сразу; решение ВСЕ РАВНО проверяется, поэтому
# при смене прошивки камеры кеш сам себя исправляет ([[_decide_inter]]).

def _enc_cache_path(key: str) -> str:
    safe = "".join(c for c in str(key) if c.isalnum() or c in "-_")
    return os.path.join(vtm_cache.cache_dir(), f"enc-{safe}.json")


def _enc_cache_load(key: str):
    """Решение, определённое при предыдущем открытии (None, если отсутствует/повреждено)."""
    try:
        with open(_enc_cache_path(key), encoding="utf-8") as f:
            return bool(json.load(f)["inter_enc"])
    except (OSError, ValueError, TypeError, KeyError):
        return None


def _enc_cache_store(key: str, value: bool) -> None:
    """АТОМАРНАЯ запись — кеш это оптимизация, не давайте ошибке останавливать работу."""
    try:
        d = vtm_cache.cache_dir()
        os.makedirs(d, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"inter_enc": bool(value)}, f)
        os.replace(tmp, _enc_cache_path(key))
    except (OSError, TypeError, ValueError):
        pass


def _is_missing_resource(exc) -> bool:
    """Устройства НЕТ в облачном списке (офлайн-камеры выводятся из списка).

    Это НЕ устаревшие метаданные — pagelist получен успешно, отсутствует
    само устройство. Поэтому в этом случае инвалидировать VTM-кеш — вред: кеш
    ведётся по аккаунту, и одна офлайн-камера стирала бы кеш всего аккаунта
    (у нас из 151 камеры 31 была офлайн — это срабатывало постоянно)."""
    return "could not find vtm resource" in str(exc or "").lower()


def keyerror_flag_path(serial: str, channel: int = 1) -> str:
    """Путь к флаговому файлу для связи со stream_manager при неверном коде."""
    return os.path.join(tempfile.gettempdir(), f"ezviz_keyerr_{serial}_{channel}.flag")


def offline_flag_path(serial: str, channel: int = 1) -> str:
    """Путь к флаговому файлу, когда камера offline (VTM не отдаёт данных)."""
    return os.path.join(tempfile.gettempdir(), f"ezviz_offline_{serial}_{channel}.flag")


class _NalDecryptBase:
    """Декодирование NAL body с AES-ECB + автоматическое определение типа шифра
    по param-set (SPS/VPS). Применяются ТРИ варианта шифра (значения clear/drop):
      A)       header ЧИСТЫЙ, body зашифрован/чистый            -> clear=hdr, drop=0
      B-hint)  чистый индикатор типа + зашифрованный ПОЛНЫЙ NAL  -> clear=0,   drop=hdr
      B-whole) весь NAL зашифрован (без индикатора, для PS)     -> clear=0,   drop=0
    """

    _PROFILES = {66, 77, 88, 100, 110, 122, 244, 44, 83, 86, 118, 128}  # H.264 profile_idc
    # СЕЛЕКТИВНЫЙ шифр: камеры шифруют только I-фрейм/IRAP + param-set NAL,
    # inter (P/B) slice оставляют чистыми. В варианте A (header чистый) расшифровываем,
    # если тип в этом списке; в B-hint определяется по индикатору (список не нужен).
    _H264_ENC_TYPES = {5, 7, 8}                              # IDR, SPS, PPS
    _HEVC_ENC_TYPES = {16, 17, 18, 19, 20, 21, 32, 33, 34}   # IRAP slices + VPS/SPS/PPS
    # IRAP = точка случайного доступа (декодер может начать с нуля отсюда).
    _H264_IRAP = {5}                                         # IDR
    _HEVC_IRAP = {16, 17, 18, 19, 20, 21}                    # BLA/IDR/CRA
    # NAL param-set выдаются и ДО IRAP (декодеру нужны SPS/PPS).
    _H264_PARAMSET = {7, 8}
    _HEVC_PARAMSET = {32, 33, 34}

    def __init__(self, key: str, *, cache_key: str | None = None):
        self.key = key.encode().ljust(16, b"\0")[:16]
        self._decrypt = None   # None=неизвестно, True=зашифровано, False=чисто
        self.key_error = False
        self.codec = None      # 'h264' | 'hevc' | None
        self._clear = 0        # начальные чистые (не расшифровываемые) байты
        self._drop = 0         # байты, отбрасываемые с начала (чистый индикатор типа)
        # Зашифрованы ли inter (P/B) slice: None=неизвестно, True/False=решение.
        # До решения выход держится В ОЧЕРЕДИ — иначе первые
        # кадры будут прочитаны неверно, и декодер выдаст ошибку переполнения.
        # Если есть кеш ([[_enc_cache_load]]), решение приходит готовым, и буферизация
        # ВООБЩЕ не происходит — первый кадр уходит сразу.
        self._cache_key = cache_key
        cached = _enc_cache_load(cache_key) if cache_key else None
        self._inter_enc = cached
        self._verifying = cached is not None   # решение из кеша проверяется
        self._inter_clear = []   # образец: 1-й байт чистого body
        self._inter_dec = []     # образец: 1-й байт AES-расшифрованного body
        self._pending = None if self._verifying else []
        self._pending_bytes = 0
        self.inter_note = None   # краткая заметка для диагностики
        # IRAP-шлюз ([[_irap_gate]])
        self._seen_irap = False
        self._gated = 0          # число сброшенных (до IRAP) slice
        self.gate_note = None    # краткая заметка для диагностики

    @property
    def encrypted(self):
        """Зашифрован ли поток: None=ещё не определено, True/False=решение.

        Открытое имя — внешний код (`check_code.py`) раньше читал `_decrypt`
        напрямую."""
        return self._decrypt

    def _aes16(self, data) -> bytes:
        return AES.new(self.key, AES.MODE_ECB).decrypt(bytes(data[:16]))

    def _decrypt_body(self, body) -> bytes:
        if not self._decrypt:  # None или False -> не расшифровываем
            return bytes(body)
        n = (min(len(body), NAL_ENCRYPTED_PREFIX) // 16) * 16
        if n == 0:
            return bytes(body)
        head = AES.new(self.key, AES.MODE_ECB).decrypt(bytes(body[:n]))
        return head + bytes(body[n:])

    def _classify(self, nal, hdr, is_paramset, body_marker) -> None:
        """Определяет вариант шифра по param-set NAL (устанавливает clear/drop/decrypt).
        is_paramset(b0)->bool: верен ли тип (SPS=7 / VPS=32); body_marker(byte)->bool:
        верна ли голова расшифрованного/чистого body (H.264 profile_idc / HEVC 0x0C)."""
        if self._decrypt is not None:
            return
        b0 = nal[0]
        ps = is_paramset(b0)
        # A) header чистый
        if ps and len(nal) > hdr and body_marker(nal[hdr]):
            self._clear, self._drop, self._decrypt = hdr, 0, False
            return                                              # чистый поток
        if ps and len(nal) >= hdr + 16 and body_marker(self._aes16(nal[hdr:hdr + 16])[0]):
            self._clear, self._drop, self._decrypt = hdr, 0, True
            return                                              # header чистый, body зашифрован
        # B-hint) чистый индикатор типа + зашифрованный полный NAL (header тоже внутри шифра)
        if len(nal) >= hdr + 16:
            dec = self._aes16(nal[hdr:hdr + 16])
            if is_paramset(dec[0]) and len(dec) > hdr and body_marker(dec[hdr]):
                self._clear, self._drop, self._decrypt = 0, hdr, True
                return
        # B-whole) весь NAL зашифрован, без индикатора
        if len(nal) >= 16:
            dec = self._aes16(nal[:16])
            if is_paramset(dec[0]) and len(dec) > hdr and body_marker(dec[hdr]):
                self._clear, self._drop, self._decrypt = 0, 0, True
                return
        # чистый header похож на param-set, но ни один вариант не подошёл -> ключ неверный
        if ps and len(nal) >= hdr + 16:
            self.key_error = True

    def _irap_gate(self, ntype: int, hdr: int) -> bool:
        """Выдать ли этот NAL? (slice до первого IRAP отбрасываются)

        NAL param-set всегда проходят — декодеру нужны SPS/PPS/VPS. После IRAP
        шлюз полностью открывается, и больше ничего не отбрасывается."""
        if self._seen_irap:
            return True
        irap = self._H264_IRAP if hdr == 1 else self._HEVC_IRAP
        pset = self._H264_PARAMSET if hdr == 1 else self._HEVC_PARAMSET
        if ntype in irap:
            self._seen_irap = True
            if self._gated:
                self.gate_note = f"до IRAP отброшено {self._gated} slice"
            return True
        if ntype in pset:
            return True
        self._gated += 1
        if self._gated > _IRAP_MAX_WAIT:
            # GOP камеры очень длинный или IRAP не отмечается — не ждём вечно.
            self._seen_irap = True
            self.gate_note = (f"IRAP не пришёл ({self._gated} slice) — шлюз "
                              f"открыт принудительно")
            return True
        return False

    # ---- выход: буферизация ПО ПОРЯДКУ до решения об inter ----

    def _put(self, out, data: bytes) -> None:
        """Выдаёт готовые байты (или кладёт в очередь до решения)."""
        if self._pending is None:
            out += data
            return
        self._pending.append((b"", data, 0))
        self._pending_bytes += len(data)
        self._guard_pending(out)

    def _inter_bytes(self, nal, hdr: int) -> bytes:
        """Inter slice — в зависимости от решения расшифровывается или остаётся чистым."""
        if self._inter_enc:
            return START + bytes(nal[:hdr]) + self._decrypt_body(nal[hdr:])
        return START + bytes(nal)

    def _put_inter(self, out, nal, hdr: int) -> None:
        """Inter slice — образец уже взят ВЫШЕ ([[_emit_nal]])."""
        if self._pending is None:       # решение есть (или пришло из кеша)
            out += self._inter_bytes(nal, hdr)
            return
        self._pending.append((b"i", bytes(nal), hdr))
        self._pending_bytes += len(nal)
        if self._inter_enc is not None:
            self._flush(out)        # решение вышло — очередь идёт ПО ПОРЯДКУ
        else:
            self._guard_pending(out)

    def _guard_pending(self, out) -> None:
        """Если inter NAL не приходит вообще (только I-фреймы) — не застреваем в ожидании."""
        if self._pending is not None and self._pending_bytes > _INTER_MAX_PENDING:
            self._inter_enc = False
            self.inter_note = "образцов не хватило -> принято как ЧИСТОЕ"
            self._flush(out)

    def _flush(self, out) -> None:
        pend, self._pending = self._pending, None
        self._pending_bytes = 0
        for kind, data, hdr in pend or ():
            out += data if kind == b"" else self._inter_bytes(data, hdr)

    @staticmethod
    def _is_inter_slice(nal, hdr: int) -> bool:
        """Это настоящий P/B slice (подходит для образца)?

        ВСЕ NAL, не входящие в СПИСОК ШИФРА (SEI, AUD, ...), попадают в inter-ветку.
        Их body — не slice header: например, 1-й байт чистого SEI (тип payload)
        всегда одинаков — он искусственно усиливал бы "чистый" голос и тянул
        фактические зашифрованные P-slice к вердикту (= ЧИСТОЕ)."""
        if hdr == 1:
            return (nal[0] & 0x1F) in (1, 2)          # H.264 non-IDR slice / partition A
        return ((nal[0] >> 1) & 0x3F) <= 9            # HEVC TRAIL/TSA/STSA/RADL/RASL

    def _sample_inter(self, nal, hdr: int) -> None:
        """ТЕСТ СТРУКТУРЫ: байты slice header ПОВТОРЯЮТСЯ (один PPS, одна
        структура), а зашифрованные байты СЛУЧАЙНЫ. Поэтому сравниваем число
        РАЗЛИЧНЫХ значений в N образцах — меньшее — правильное чтение.
        Тест "допустимости" по одному байту не работает: случайный байт тоже в ~87%
        случаев даёт допустимый ue(v); а доля различных значений резко различает
        (измерение: чисто=1.00 vs deshifr=0.06)."""
        body = nal[hdr:]
        if len(body) < 16:
            return
        self._inter_clear.append(body[0])
        self._inter_dec.append(self._aes16(body)[0])
        if len(self._inter_clear) >= _INTER_SAMPLES:
            self._decide_inter()

    def _decide_inter(self) -> None:
        """Делает решение по образцам (и записывает в кеш)."""
        n = len(self._inter_clear)
        d_clear = len(set(self._inter_clear)) / n
        d_dec = len(set(self._inter_dec)) / n
        # При равенстве оставляем ЧИСТОЕ (безопаснее: расшифровывать чистый P-фрейм
        # — значит сломать его), переходим к расшифровке только при явном различии.
        fresh = d_dec + _INTER_MARGIN < d_clear
        if self._verifying:
            # Кеш был использован (буферизация пропущена). Теперь проверяем:
            # если верно — молча идём дальше, если неверно — ИСПРАВЛЯЕМ — при этом
            # первые несколько P-фреймов уйдут битыми (~0.5s), потом
            # чистые. Компромисс: вместо экономии ~0.7с при каждом открытии —
            # короткая деградация в редком случае, когда ПОВЕДЕНИЕ камеры ИЗМЕНИЛОСЬ.
            self._verifying = False
            if fresh != self._inter_enc:
                self._inter_enc = fresh
                self.inter_note = (f"кеш был НЕВЕРНЫМ -> исправлено: "
                                   f"inter={'ЗАШИФР' if fresh else 'ЧИСТОЕ'} "
                                   f"(чист={d_clear:.2f} расшифр={d_dec:.2f})")
                if self._cache_key:
                    _enc_cache_store(self._cache_key, fresh)
            return
        self._inter_enc = fresh
        self.inter_note = (f"inter={'ЗАШИФР' if self._inter_enc else 'ЧИСТОЕ'} "
                           f"(чист={d_clear:.2f} расшифр={d_dec:.2f}, {n} образцов)")
        if self._cache_key:
            _enc_cache_store(self._cache_key, self._inter_enc)

    def _emit_nal(self, out, nal) -> None:
        if self._decrypt is None:
            return  # не выдаём, пока шифр не определён
        # Вопрос об inter есть ТОЛЬКО в варианте "A-body" (header чистый, body зашифрован).
        # В чистом потоке / B-hint / B-whole вопроса нет -> выключаем буферизацию.
        # Здесь очередь ещё пуста (выхода не было до определения `_decrypt`).
        if self._pending is not None and not (self._decrypt and self._clear and not self._drop):
            self._pending = None
        if not self._decrypt:                       # чистый поток
            # Шлюз нужен и в чистом потоке — поток всё равно начинается
            # посередине GOP ([[_irap_gate]]).
            h = self._clear or 2
            nt = (nal[0] & 0x1F) if h == 1 else ((nal[0] >> 1) & 0x3F)
            if not self._irap_gate(nt, h):
                return
            self._put(out, START + bytes(nal))
            return
        if self._drop:
            # ВНИМАНИЕ: в B-hint и B-whole NAL header ЗАШИФРОВАН, то есть тип
            # прочитать заранее нельзя -> IRAP-шлюз здесь НЕ ПРИМЕНЯЕТСЯ.
            # B-hint + СЕЛЕКТИВНЫЙ шифр (каждый NAL отдельно). В NAL I-фрейм/param-set
            # есть чистый индикатор типа (дублирующий header): если зашифрован, расшифровка
            # AES совпадает с header; если очень короткий (PPS) — дублирующий header без AES. P-фрейм без индикатора.
            hdr = self._drop
            if len(nal) >= hdr + 16 and self._aes16(nal[hdr:hdr + 16])[:hdr] == bytes(nal[:hdr]):
                self._put(out, START + self._decrypt_body(nal[hdr:]))  # зашифрован -> полный NAL
            elif len(nal) >= 2 * hdr and bytes(nal[hdr:2 * hdr]) == bytes(nal[:hdr]):
                self._put(out, START + bytes(nal[hdr:]))        # чистый, дублирующий индикатор
            else:
                self._put(out, START + bytes(nal))              # обычный чистый NAL (P-фрейм)
            return
        # A (header чистый, body зашифрован) или B-whole (весь NAL зашифрован)
        hdr = self._clear
        if hdr == 0:                                    # B-whole: расшифровка всего NAL
            self._put(out, START + self._decrypt_body(nal))
            return
        if len(nal) <= hdr:
            self._put(out, START + bytes(nal))
            return
        # Вариант A: IRAP + param-set ВСЕГДА зашифрованы. Inter (P/B) slice —
        # ЗАВИСИТ ОТ КАМЕРЫ — поэтому не гадаем, определяем по потоку
        # ([[_sample_inter]]). До решения выход держится в очереди.
        ntype = (nal[0] & 0x1F) if hdr == 1 else ((nal[0] >> 1) & 0x3F)
        enc = self._H264_ENC_TYPES if hdr == 1 else self._HEVC_ENC_TYPES
        # ОБРАЗЕЦ берётся НЕЗАВИСИМО ОТ ШЛЮЗА — тогда решение будет готово
        # к моменту ожидания IRAP, и после открытия шлюза нет дополнительной задержки.
        if (ntype not in enc and self._is_inter_slice(nal, hdr)
                and (self._inter_enc is None or self._verifying)):
            self._sample_inter(nal, hdr)
        if self._inter_enc is not None and self._pending is not None:
            self._flush(out)          # решение прибыло — освобождаем очередь
        if not self._irap_gate(ntype, hdr):
            return
        if ntype in enc:
            self._put(out, START + bytes(nal[:hdr]) + self._decrypt_body(nal[hdr:]))
        else:
            self._put_inter(out, nal, hdr)              # inter (P/B) slice

    def _emit(self, out: bytearray, nal) -> None:  # HEVC RTP (2-байтовый NAL header, VPS=тип 32)
        if len(nal) < 2:
            return
        self._classify(nal, 2, lambda b: ((b >> 1) & 0x3F) == 32, lambda x: x == 0x0C)
        self._emit_nal(out, nal)


class _RtpSeqMixin:
    """Контроль номера последовательности RTP.

    Если потеряется кусок FU, остальные куски склеятся, и NAL продолжится. В
    зашифрованном body это СДВИГАЕТ блоки AES по 16 байт — весь NAL после
    места потери превращается в мусор, и декодеру приходит битый slice. При
    обнаружении разрыва собираемый NAL отбрасывается (декодер лучше восстанавливается без него).

    Seq отслеживается по ПАРЕ (SSRC, PT). В реальном потоке (Hik-Connect,
    измерено) внутри одного SSRC идут video (PT 96), metadata (PT 112) и
    audio (PT 0), и У КАЖДОГО СВОЙ счётчик seq. Счёт только по SSRC при
    каждом переключении PT давал ложные "разрывы" (706 из 2318 пакетов)."""

    def _init_seq(self) -> None:
        self._last_seq = {}    # {(ssrc, pt): последний seq}
        self.seq_gaps = 0      # диагностика
        self.dup_packets = 0

    def _check_seq(self, pkt: bytes) -> bool:
        """False — пакет дубликат (отбрасывается). При разрыве текущий FU аннулируется."""
        if len(pkt) < 12:
            return True
        seq = (pkt[2] << 8) | pkt[3]
        stream = (bytes(pkt[8:12]), pkt[1] & 0x7F)      # (SSRC, PT)
        last = self._last_seq.get(stream)
        self._last_seq[stream] = seq
        if last is None:
            return True
        if seq == last:
            self.dup_packets += 1
            return False
        # Разрыв в VIDEO-потоке затрагивает собираемый NAL (потеря audio
        # пакета — отбрасывать видео-FU было бы лишним повреждением).
        if seq != (last + 1) & 0xFFFF and stream[1] == HEVC_VIDEO_PT:
            self.seq_gaps += 1
            self._cur = None   # половинчатый NAL — куска нет, отбрасываем
        return True


class HevcRtpDecryptor(_RtpSeqMixin, _NalDecryptBase):
    """Преобразует RTP/HEVC пакеты в Annex-B и декодирует NAL body."""

    def __init__(self, key: str, *, cache_key: str | None = None):
        super().__init__(key, cache_key=cache_key)
        self._cur = None       # текущая FU-сборка (фрагмент)
        self._init_seq()

    def feed(self, rtp_packet: bytes) -> bytes:
        """Возвращает байты Annex-B из одного RTP-пакета (может быть пустым)."""
        if not self._check_seq(rtp_packet):
            return b""
        if (rtp_packet[1] & 0x7F) != HEVC_VIDEO_PT:
            return b""
        pl = rtp_payload(rtp_packet)
        if len(pl) < 3:
            return b""
        out = bytearray()
        nal_type = (pl[0] >> 1) & 0x3F
        if nal_type == 49:  # FU — фрагментированный NAL
            fu = pl[2]
            start_bit = fu >> 7
            end_bit = (fu >> 6) & 1
            fu_type = fu & 0x3F
            if start_bit:
                nh0 = ((pl[0] & 0x81) | (fu_type << 1)) & 0xFF
                self._cur = bytearray([nh0, pl[1]])
                self._cur += pl[3:]
            elif self._cur is not None:
                self._cur += pl[3:]
            if end_bit and self._cur is not None:
                self._emit(out, self._cur)
                self._cur = None
        elif nal_type == 48:  # AP — несколько NAL в одном пакете
            i = 2
            while i + 2 <= len(pl):
                sz = int.from_bytes(pl[i:i + 2], "big")
                i += 2
                self._emit(out, pl[i:i + sz])
                i += sz
        else:  # одиночный NAL (VPS/SPS/PPS/slice)
            self._emit(out, pl)
        return bytes(out)


class H264RtpDecryptor(_RtpSeqMixin, _NalDecryptBase):
    """Преобразует RTP/H.264 пакеты в Annex-B и декодирует NAL body.
    H.264 NAL header 1 байт; FU-A=28, STAP-A=24, одиночный NAL=1..23."""

    def __init__(self, key: str, *, cache_key: str | None = None):
        super().__init__(key, cache_key=cache_key)
        self._cur = None
        self._init_seq()

    def _emit(self, out: bytearray, nal):  # override: 1-байтный NAL header, SPS=тип 7
        if len(nal) < 1:
            return
        self._classify(nal, 1, lambda b: (b & 0x1F) == 7, lambda x: x in self._PROFILES)
        self._emit_nal(out, nal)

    def feed(self, rtp_packet: bytes) -> bytes:
        if not self._check_seq(rtp_packet):
            return b""
        if (rtp_packet[1] & 0x7F) != HEVC_VIDEO_PT:
            return b""
        pl = rtp_payload(rtp_packet)
        if len(pl) < 1:
            return b""
        out = bytearray()
        t = pl[0] & 0x1F
        if t == 28:  # FU-A — фрагментированный NAL
            if len(pl) < 2:
                return b""
            fu = pl[1]
            start_bit = fu >> 7
            end_bit = (fu >> 6) & 1
            fu_type = fu & 0x1F
            if start_bit:
                self._cur = bytearray([(pl[0] & 0xE0) | fu_type])
                self._cur += pl[2:]
            elif self._cur is not None:
                self._cur += pl[2:]
            if end_bit and self._cur is not None:
                self._emit(out, self._cur)
                self._cur = None
        elif t == 24:  # STAP-A — несколько NAL в одном пакете
            i = 1
            while i + 2 <= len(pl):
                sz = int.from_bytes(pl[i:i + 2], "big")
                i += 2
                self._emit(out, pl[i:i + sz])
                i += sz
        else:  # одиночный NAL (SPS/PPS/slice)
            self._emit(out, pl)
        return bytes(out)


def detect_rtp_codec(payload0: int):
    """Codec по первому байту RTP video payload: 'h264' | 'hevc' | None.
    Только по ОДНОЗНАЧНЫМ маркерам (FU или param-set); для неясного (обычный slice)
    пакета возвращаем None -> вызывающий проверит следующий пакет."""
    h264_t = payload0 & 0x1F
    hevc_t = (payload0 >> 1) & 0x3F
    # Однозначный H.264: FU-A=28, STAP-A=24, SPS=0x67, PPS=0x68, IDR=0x65
    if h264_t in (28, 24) or payload0 in (0x67, 0x68, 0x65):
        return "h264"
    # Однозначный HEVC: FU=49, AP=48, VPS=0x40, SPS=0x42, PPS=0x44
    # (HEVC IDR 0x26/0x28 не используется — конфликтует с H.264 SEI/PPS;
    #  HEVC IDR всё равно большой -> определяется через FU=49)
    if hevc_t in (49, 48) or payload0 in (0x40, 0x42, 0x44):
        return "hevc"
    return None  # неясный


class PsStreamDecryptor(_NalDecryptBase):
    """Демультиплексирует MPEG-PS поток и декодирует NAL body в ES.
    Автоматически определяет H.264 и HEVC. (PS-декодер библиотеки не работает
    с бесконечным видео-PES, поэтому делаем сами.)"""

    _PROFILES = {66, 77, 88, 100, 110, 122, 244, 44, 83, 86, 118, 128}

    def __init__(self, key: str, *, cache_key: str | None = None):
        super().__init__(key, cache_key=cache_key)
        self._buf = bytearray()   # не-демультиплексированный остаток PS
        self._es = bytearray()    # выделенный ES (остаток, ещё не разбитый на NAL)
        self._enc_off = None      # offset начала шифра: 0=весь NAL, 1=H.264, 2=после HEVC header
        self._undecided = []      # временно храним NAL, пока не определены codec/шифр
        # (имя `_pending` ЗАНЯТО — это очередь inter-решения родительского класса)
        self.buf_resyncs = 0      # число resync при превышении предела буфера

    def _emit(self, out: bytearray, nal) -> None:  # override: codec/variant-aware
        if not nal:
            return
        if self._decrypt is None:                 # codec/шифр ещё не определён
            self._undecided.append(bytes(nal))
            res = self._decide(self._undecided)
            if res == "keyerror":
                self.key_error = True
                self._undecided = []
                return
            if res is None:
                return                            # до определения не выдаём мусор
            self.codec, self._enc_off, self._decrypt = res
            # В терминах родительского класса: off>0 = вариант A (header чистый), off=0 = B-whole.
            self._clear, self._drop = self._enc_off, 0
            for n in self._undecided:             # выдаём собранные NAL
                self._emit_nal(out, n)
            self._undecided = []
            return
        self._emit_nal(out, nal)

    # Выход через `_emit_nal` родительского класса — та же логика, что для RTP:
    # в варианте A IRAP + param-set всегда расшифровываются, inter (P/B) slice —
    # определяется по потоку ([[_sample_inter]]). Раньше PS-ветка расшифровывала ВСЕ NAL —
    # на камере с селективным шифром каждый P-фрейм ломался.

    def _aes16(self, data) -> bytes:
        return AES.new(self.key, AES.MODE_ECB).decrypt(bytes(data[:16]))

    @staticmethod
    def _h264_valid(b: int) -> bool:
        return (b & 0x80) == 0 and (b & 0x1F) in (1, 5, 6, 7, 8, 9)

    @staticmethod
    def _hevc_valid(b: int) -> bool:
        return (b & 0x80) == 0 and ((b >> 1) & 0x3F) in (0, 1, 2, 4, 19, 20, 21, 32, 33, 34, 39, 40)

    def _corrob(self, big, decoded) -> bool:
        """Подтверждение трактовки: являются ли большинство NAL header в образце
        (или расшифрованных header) подлинными для данного codec (защита от единичного ложного совпадения)."""
        hits = sum(decoded)
        return hits >= max(2, len(big) // 2)

    def _decide(self, nals):
        """По собранным NAL определяет codec + offset шифра + зашифрован/чист. 4 трактовки:
        header чистый (A, off=hdr) или весь NAL зашифрован (B, off=0), H.264 или HEVC.
        Находит SPS(H.264)/VPS(HEVC) и подтверждает маркером body (независимо от NRI).
        Возвращает: (codec, enc_off, decrypt) | 'keyerror' | None."""
        big = [n for n in nals if len(n) >= 18]
        if len(big) < 3:
            return None
        PROF = self._PROFILES
        decB = [self._aes16(n) for n in big]      # вариант B: расшифрованная голова всего NAL
        for i, n in enumerate(big):
            # --- A) header ЧИСТЫЙ, body зашифрован/чистый ---
            if (n[0] & 0x80) == 0 and (n[0] & 0x1F) == 7:           # H.264 SPS
                if n[1] in PROF and self._corrob(big, [self._h264_valid(m[0]) for m in big]):
                    return ("h264", 1, False)                       # чистый поток
                if self._aes16(n[1:17])[0] in PROF and self._corrob(big, [self._h264_valid(m[0]) for m in big]):
                    return ("h264", 1, True)
            if (n[0] & 0x80) == 0 and ((n[0] >> 1) & 0x3F) == 32:   # HEVC VPS
                if n[2] == 0x0C and self._corrob(big, [self._hevc_valid(m[0]) for m in big]):
                    return ("hevc", 2, False)                       # чистый поток
                if self._aes16(n[2:18])[0] == 0x0C and self._corrob(big, [self._hevc_valid(m[0]) for m in big]):
                    return ("hevc", 2, True)
            # --- B) ВЕСЬ NAL зашифрован (включая header) ---
            d = decB[i]
            if (d[0] & 0x1F) == 7 and d[1] in PROF and self._corrob(big, [self._h264_valid(x[0]) for x in decB]):
                return ("h264", 0, True)
            if ((d[0] >> 1) & 0x3F) == 32 and d[2] == 0x0C and self._corrob(big, [self._hevc_valid(x[0]) for x in decB]):
                return ("hevc", 0, True)
        if len(big) >= 24:        # NAL достаточно, но ничего не подошло -> ключ неверный
            return "keyerror"
        return None

    def _demux(self):
        """Выделяет из PS payload видео PES (0xE0-EF) и дописывает в _es."""
        b = self._buf
        n = len(b)
        pos = 0
        while True:
            sc = b.find(b"\x00\x00\x01", pos)
            if sc < 0 or sc + 4 > n:
                break
            code = b[sc + 3]
            if code == 0xBA:  # pack header
                if sc + 14 > n:
                    break
                end = sc + 14 + (b[sc + 13] & 0x07)  # + stuffing
                if end > n:
                    break
                pos = end
            elif code == 0xB9:  # MPEG end
                pos = sc + 4
            elif 0xE0 <= code <= 0xEF:  # video PES
                if sc + 9 > n:
                    break
                length = (b[sc + 4] << 8) | b[sc + 5]
                payload_start = sc + 9 + b[sc + 8]  # 9 = 6 + 2 flags + 1 hdrlen
                if payload_start > n:
                    break
                if length > 0:
                    end = sc + 6 + length
                    if end > n:
                        break
                    self._es += b[payload_start:end]
                    pos = end
                else:  # бесконечный: до следующего start code
                    nxt = b.find(b"\x00\x00\x01", payload_start)
                    if nxt < 0:
                        break  # неполный — ждём следующий chunk
                    self._es += b[payload_start:nxt]
                    pos = nxt
            else:  # другой stream (audio/system/PSM) — пропускаем по длине
                if sc + 6 > n:
                    break
                length = (b[sc + 4] << 8) | b[sc + 5]
                end = sc + 6 + length
                if end > n:
                    break
                pos = end
        self._buf = b[pos:]

    def _process_es(self) -> bytes:
        """Декодирует полные NAL в _es и возвращает Annex-B."""
        e = self._es
        starts = []
        i = e.find(b"\x00\x00\x01", 0)
        while i >= 0:
            starts.append(i)
            i = e.find(b"\x00\x00\x01", i + 3)
        if len(starts) < 2:
            return b""
        out = bytearray()
        for k in range(len(starts) - 1):
            nal = e[starts[k] + 3:starts[k + 1]]  # header(2) + body
            # если следующий start code 4-байтовый, срезаем последний 0x00
            if nal and nal[-1:] == b"\x00":
                nal = nal[:-1]
            self._emit(out, nal)
        self._es = e[starts[-1]:]  # остаётся последний неполный NAL
        return bytes(out)

    def _cap(self) -> None:
        """Если буферы превысили предел — RESYNC на последний start code.

        В нормальном потоке буферы опустошаются при каждом `feed`. Но при входе
        без start code или с повреждением `_demux` не может выделить ничего,
        и `_buf`/`_es` росло бы бесконечно — для долгоживущего процесса это утечка памяти."""
        for name in ("_buf", "_es"):
            b = getattr(self, name)
            if len(b) > _MAX_PS_BUF:
                i = b.rfind(b"\x00\x00\x01")
                setattr(self, name, b[i:] if i > 0 else bytearray())
                self.buf_resyncs += 1

    def feed(self, ps_chunk: bytes) -> bytes:
        self._buf += ps_chunk
        self._demux()
        out = self._process_es()
        self._cap()
        return out


class PacedWriter:
    """Пишет, СГЛАЖИВАЯ облачные bursts (jitter buffer) + не блокирует облачный сокет.

    Решает ДВЕ проблемы.

    1) ЗАВИСАНИЯ. Облако шлёт кадры не ровно, а GOP за GOP. Измерено (одна
       камера, 500 кадров): **491 кадр пришёл за <10 ms, остальные 9 интервалов
       ~2000 ms** — то есть весь GOP мгновенно, потом 2с тишины. Если передавать
       кадр по мере прихода, зритель 2с видит застывшее изображение, потом 50 кадров
       проносятся "быстрой перемоткой". Приложение на телефоне выглядит плавно,
       потому что буферизует. Поэтому кадры ЗДЕСЬ — пока ещё В СЖАТОМ
       виде (GOP ~0.2 MB, на несжатых кадрах было бы 50 x 2.7 MB) —
       буферизуются и выдаются с измеренной скоростью.

    2) BACKPRESSURE. Раньше в сокет вывода писал САМ ОБЛАЧНЫЙ READER-THREAD:
       если ffmpeg/зритель замедлился, `write()` блокируется, облачный сокет
       не читается, и облако бросает пакеты -> изображение портится. Теперь запись
       в отдельном thread; при заполнении очереди кадры отбрасываются до границы keyframe
       (без обрыва посередине), а облачный reader никогда не ждёт.
    """

    MAX_BYTES = 8 << 20        # лимит очереди (сжатые байты); через PACE_QUEUE_MB
    # для сервисного использования (один зритель, а не grid из 23 плиток)
    # достаточно 1–2 МБ — глубина буфера TARGET_S уже ограничивает задержку.
    if os.environ.get("PACE_QUEUE_MB"):
        try:
            MAX_BYTES = int(int(float(os.environ["PACE_QUEUE_MB"]) * 1024 * 1024))
        except ValueError:
            pass
    TARGET_S = 1.0             # целевая глубина буфера
    RATE_WINDOW_S = 4.0        # скорость прихода измеряется в этом окне
    MIN_STEP, MAX_STEP = 1 / 60.0, 1 / 4.0

    def __init__(self, wfile, codec: str | None, paced: bool = True):
        self._w = wfile
        self._paced = paced
        self._hevc = codec != "h264"
        self._q = []               # [(bytes, это кадр, это keyframe)]
        self._bytes = 0
        self._rx = []              # моменты прихода кадров
        self._cur = bytearray()    # собираемый кадр
        self._cur_key = False
        self._cur_has_vcl = False
        self._lock = threading.Lock()
        self._ev = threading.Event()
        self.failed = False
        self.dropped = 0
        self._stop = False
        self._th = threading.Thread(target=self._run, daemon=True)

    # ---- Разбиение Annex-B: находим границы КАДРА по NAL ----
    def _nal_kind(self, b0: int, b1: int) -> tuple[bool, bool]:
        """(vcl, key) — по типу NAL."""
        if self._hevc:
            t = (b0 >> 1) & 0x3F
            return t <= 31, 16 <= t <= 21
        t = b0 & 0x1F
        return 1 <= t <= 5, t == 5

    def start(self):
        self._th.start()
        return self

    def feed(self, data: bytes) -> None:
        """Расшифрованные байты — разбиваем на кадры и кладём в очередь."""
        if not self._paced:
            self._push(data, frame=True, key=False)
            return
        pos = 0
        while True:
            i = data.find(b"\x00\x00\x01", pos)
            if i < 0:
                self._cur += data[pos:]
                break
            hdr = i + 3
            if hdr < len(data):
                vcl, key = self._nal_kind(data[hdr], data[hdr + 1] if hdr + 1 < len(data) else 0)
                # Новый VCL NAL — предыдущий кадр закончился (считаем
                # кадр = один slice; при нескольких slice поправит обратная связь глубины).
                if vcl and self._cur_has_vcl:
                    start = i - 1 if i > 0 and data[i - 1] == 0 else i
                    self._cur += data[pos:start]
                    self._flush_frame()
                    pos = start
                    continue
                if key:
                    self._cur_key = True
                if vcl:
                    self._cur_has_vcl = True
            self._cur += data[pos:hdr]
            pos = hdr
        if len(self._cur) > (2 << 20):     # защита: граница не найдена
            self._flush_frame()

    def _flush_frame(self) -> None:
        if not self._cur:
            return
        self._push(bytes(self._cur), frame=True, key=self._cur_key)
        self._cur = bytearray()
        self._cur_key = False
        self._cur_has_vcl = False

    def _push(self, data: bytes, *, frame: bool, key: bool) -> None:
        with self._lock:
            self._q.append((data, frame, key))
            self._bytes += len(data)
            if frame:
                self._rx.append(time.monotonic())
            # Переполнение — отбрасываем кадры ДО СЛЕДУЮЩЕГО KEYFRAME
            # (обрыв посередине даст декодеру битый GOP; а это чистый скачок).
            if self._bytes > self.MAX_BYTES:
                while len(self._q) > 1:
                    d, _f, _k = self._q.pop(0)
                    self._bytes -= len(d)
                    self.dropped += 1
                    if self._q[0][2]:          # впереди keyframe — останавливаемся
                        break
        self._ev.set()

    def _step(self) -> float:
        now = time.monotonic()
        while self._rx and now - self._rx[0] > self.RATE_WINDOW_S:
            self._rx.pop(0)
        # ВНИМАНИЕ: скорость прихода нельзя измерить внутри burst (50 кадров
        # приходят мгновенно). Поэтому измерение используется только если оно
        # охватывает хотя бы несколько GOP; до этого стартуем с 25 fps, и разницу
        # поправляет обратная связь по глубине буфера.
        span = now - self._rx[0] if self._rx else 0.0
        if len(self._rx) > 20 and span > 2.0:
            nominal = span / len(self._rx)
        else:
            nominal = 0.04
        nominal = max(self.MIN_STEP, min(self.MAX_STEP, nominal))
        depth = len(self._q) * nominal
        if depth > self.TARGET_S * 2:
            return nominal * 0.85          # отстаём — чуть быстрее
        if depth < self.TARGET_S * 0.5:
            return nominal * 1.15          # буфер мелкий — чуть медленнее
        return nominal

    def _run(self) -> None:
        due = time.monotonic()
        while True:
            with self._lock:
                item = self._q.pop(0) if self._q else None
                if item:
                    self._bytes -= len(item[0])
            if item is None:
                if self._stop:
                    return                 # выходим, когда очередь ПУСТА
                self._ev.wait(0.05)
                self._ev.clear()
                due = time.monotonic()
                continue
            try:
                self._w.write(item[0])
            except Exception:
                self.failed = True
                return
            if self._paced and item[1]:
                due += self._step()
                delay = due - time.monotonic()
                if delay > 0:
                    time.sleep(min(delay, 1.0))
                elif delay < -1.0:
                    due = time.monotonic()   # слишком отстаём — сбрасываем таймер

    def close(self, drain: float = 3.0) -> None:
        """Останавливается после опорожнения очереди (чтобы при закрытии не
        терялись последние кадры), но не ждёт дольше `drain` секунд."""
        self._flush_frame()
        self._stop = True
        self._ev.set()
        if self._th.is_alive():
            self._th.join(timeout=drain)


_orig_build_vtm_url = None


def set_substream(enable: bool) -> None:
    """Запрашивает у облака МАЛЫЙ (substream) поток — `stream=2`
    вместо `stream=1` в URL VTM.

    Зачем: в grid каждая плитка ~250-500 px на экране, а камера шлёт 2560x1440 —
    это в ~30 раз лишних пикселей, которые экран и не покажет. Измерение
    (та же стена, 23 плитки): на основном потоке изображение рассыпалось и
    застыло, с substream каждая плитка **640x360, ~0.11 Mbit/s** (основной ~4 Mbit/s —
    в 36 раз меньше), и всё декодировано чисто. На слабом uplink ещё и
    падает потеря пакетов (зелёные линии/артефакты).

    Substream есть не на всех камерах (например, батарейные модели) —
    в таком случае облако отдаёт основной поток, то есть вреда нет.
    """
    global _orig_build_vtm_url
    import re as _re
    if _orig_build_vtm_url is None:
        _orig_build_vtm_url = _cs.build_vtm_url          # ИСХОДНАЯ копия (один раз)
    if enable:
        def _sub(*a, **kw):
            return _re.sub(r"(?<=[?&])stream=1(?=&|$)", "stream=2",
                           _orig_build_vtm_url(*a, **kw))
        _cs.build_vtm_url = _sub
    else:
        _cs.build_vtm_url = _orig_build_vtm_url


def _make_client():
    import re
    with open(get_active().token_file, encoding="utf-8") as f:
        token = json.load(f)
    client = EzvizClient(token.get("username"), None, token.get("api_url"), token=token)
    # Тип клиента — без него pagelist не вернёт все ресурсы (зависит от платформы)
    try:
        client._session.headers.update({"clientType": _CLIENT_TYPE, "lang": "en-US"})
    except Exception:
        pass
    # Hik-Connect возвращает authAddr = "https://null" -> выводим из region
    # (apiiSGP.hik-connect.com -> sgpauth.ezvizlife.com). Для EZVIZ тоже верно.
    su = (token.get("service_urls") or {})
    if not su.get("authAddr") or "null" in str(su.get("authAddr")).lower():
        m = re.match(r"apii([a-z]+)\.", str(token.get("api_url", "")))
        if m:
            client._token.setdefault("service_urls", {})["authAddr"] = \
                f"https://{m.group(1)}auth.ezvizlife.com"
    return client


def _device_names(client):
    """{serial: имя_устройства} — имена из приложения (имя NVR/камеры)."""
    tok = getattr(client, "_token", {}) or {}
    api = tok.get("api_url")
    sess = _rq.Session()
    sess.headers.update({
        "clientType": _CLIENT_TYPE, "lang": "en-US",
        "featureCode": feature_code(),        # специфичен для этой установки ([[identity]])
        "sessionId": str(tok.get("session_id")),
    })
    names = {}
    offset = 0
    for _ in range(40):
        r = sess.get(f"https://{api}/v3/userdevices/v1/devices/pagelist",
                     params={"filter": "CONNECTION", "groupId": -1, "limit": 50, "offset": offset},
                     timeout=25)
        d = r.json()
        di = d.get("deviceInfos") or []
        for x in di:
            names[x.get("deviceSerial")] = (x.get("name") or "").strip()
        page = d.get("page") or {}
        if not di or not page.get("hasNext"):
            break
        offset += len(di)
    return names


def list_cameras(client=None):
    """Список реальных камер: [(serial, channel, name)].
    Имя = "Имя устройства — Имя канала" (чтобы совпадало с именем в приложении).
    Берётся из ресурсов VTM — пустые каналы NVR (камера не подключена) не показываются.

    `client` — `EzvizClient` pyezvizapi (с token). Высокоуровневый API передавал свой
    объект `CloudClient`, в котором нет `_token`: запрос улетал
    на `https://none/...` и давал `NameResolutionError`. Поэтому если передан
    неподходящий объект, собираем сами из token-файла."""
    if client is None or not getattr(client, "_token", None):
        client = _make_client()
    dev_names = _device_names(client)
    res = _cs.get_vtm_page_list(client).get("resourceInfos", []) or []
    cams = []
    for r in res:
        try:
            ch = int(r.get("localIndex"))
        except (TypeError, ValueError):
            continue
        if ch < 1:  # localIndex 0 = само устройство (NVR), не камера
            continue
        serial = r.get("deviceSerial")
        chname = (r.get("resourceName") or "").strip()
        devname = dev_names.get(serial, "")
        if devname and chname and devname.lower() not in chname.lower():
            label = f"{devname} - {chname}"   # ASCII '-' (в окне cv2 '—' не отображается)
        else:
            label = chname or devname or f"{serial} CH{ch}"
        cams.append((serial, ch, label))
    cams.sort(key=lambda x: (x[0], x[1]))
    return cams


STREAM_PROBE_TIMEOUT = 6.0     # в режиме "auto" substream ожидается именно столько
PACE_FRAMES = os.environ.get("PACE_FRAMES", "1") not in ("0", "false", "off", "")
# Размер кольца _StreamSession: хвост от последнего keyframe. 1.5 МБ — ~1–2 GOP
# (GOP main ~0.2 МБ). Больше — больше "готовность", меньше — меньше память.
_RING_MAX_BYTES = int(1.5 * 1024 * 1024)


def _prime_packets(pkts):
    """Читает начальные пакеты, чтобы определить формат (RTP/PS/TS) и codec
    (H.264/HEVC). Пакеты возвращаются вызывающему — они ещё пригодятся."""
    buffered, is_rtp, codec = [], None, None
    for pkt in pkts:
        b = bytes(pkt.body)
        buffered.append(b)
        if is_rtp is None:
            is_rtp = len(b) >= 1 and (b[0] >> 6) == 2
        if not is_rtp:
            break  # MPEG-PS или TS
        if (b[1] & 0x7F) == HEVC_VIDEO_PT:
            pl = rtp_payload(b)
            if len(pl) >= 1:
                codec = detect_rtp_codec(pl[0])
                if codec:
                    break
        # сканируем до keyframe/однозначного маркера (для длинного GOP)
        if len(buffered) >= 400:
            break
    return buffered, is_rtp, codec


class _StreamSession:
    """Общая фоновая VTM-сессия на канал (одна на воркер, не на запрос).

    Читатель-тред ([[serve]]: `_reader`) держит облачный поток открытым
    постоянно и дописывает в ЛОГ уже преобразованные (расшифрованные)
    данные. Лог — это и есть КОЛЬЦО: окно от последнего KEYFRAME (Annex-B)
    или байтовое окно (TS). Клиент в любой момент: снимает снимок лога
    (готовый GOP-хвост с keyframe-входом — декодер стартует с него) и
    дальше потребляет только новые записи.

    Зачем: раньше каждый HTTP-запрос (особенно /frame) открывал НОВУЮ
    VTM-сессию — handshake с облаком занимал 1–3 с вдобавок к ожиданию
    I-кадра. Теперь подключение клиента к «тёплому» потоку мгновенное.

    Если сессия падает (сбой облака, обрыв) — читатель перезапускает её;
    клиенты замечают смену генерации (gen) и берут новый снимок."""

    def __init__(self):
        self.cond = threading.Condition()
        self.log = []            # (seq, data, is_keyframe)
        self.seq = 0
        self.gen = 0             # растёт при каждом (пере)запуске сессии
        self.annexb = True
        self.codec = None
        self.last_data = 0.0

    def begin(self, annexb: bool, codec) -> None:
        """(Пере)запуск сессии: свежий лог, новая генерация."""
        with self.cond:
            self.gen += 1
            self.annexb = annexb
            self.codec = codec
            self.log.clear()
            self.cond.notify_all()

    @staticmethod
    def _keyframe_in(data: bytes) -> bool:
        """Есть ли keyframe NAL в куске Annex-B (HEVC 16-21 / H.264 IDR=5)."""
        pos = 0
        while True:
            i = data.find(b"\x00\x00\x01", pos)
            if i < 0:
                return False
            hdr = i + 3
            if hdr < len(data):
                t = data[hdr]
                if ((t >> 1) & 0x3F) in range(16, 22) or (t & 0x1F) == 5:
                    return True
            pos = hdr

    def append(self, data: bytes) -> None:
        """Данные от читателя: в лог + разбудить клиентов."""
        if not data:
            return
        kf = self._keyframe_in(data) if self.annexb else False
        with self.cond:
            self.seq += 1
            self.log.append((self.seq, data, kf))
            self.last_data = time.time()
            self._trim()
            self.cond.notify_all()

    def _trim(self) -> None:
        """Под lock. Для Annex-B отбрасываем ДО (не включая) ПЕРВОГО keyframe
        (он — точка входа декодера); для TS — по байтам (маркера keyframe в
        TS нет). Жёсткий байтовый лимит — fallback, если keyframe не нашёлс
        (повреждённый поток) или кольцо всё равно велико."""
        total = sum(len(d) for _s, d, _k in self.log)
        if self.annexb and total > _RING_MAX_BYTES:
            i = 0
            for j, (_s, _d, k) in enumerate(self.log):
                if k:
                    i = j
                    break
            if i:
                del self.log[:i]
        total = sum(len(d) for _s, d, _k in self.log)
        while self.log and total > _RING_MAX_BYTES:
            _s, d, _k = self.log.pop(0)
            total -= len(d)


def serve(serial: str, port: int, key: str, channel: int = 1,
          substream: bool = False, stream_mode: str | None = None):
    """Локальный декодирующий HTTP-прокси для одной камеры.

    `stream_mode`: "main" | "sub" | "auto". "auto" — сначала запрашивается МАЛЫЙ
    поток (substream), и если он не даёт кадр за [[STREAM_PROBE_TIMEOUT]],
    откат к основному потоку.

    Зачем "auto": substream снижает трафик в ~36 раз
    ([[set_substream]]), но его нет не на всех камерах (батарейные модели,
    некоторые каналы NVR). Жёсткий `--stream=sub` на такой камере даст чёрный экран;
    жёсткий "main" будет зря тянуть 1440p в grid. `hikcloudstream`
    тоже идёт по этому пути: пробует substream и при необходимости
    откатывается к основному."""
    client = _make_client()
    mode = stream_mode or ("sub" if substream else "main")
    if mode not in ("main", "sub", "auto"):
        mode = "main"
    # Решение об inter-шифре кешируется по ЭТОЙ КАМЕРЕ ([[_enc_cache_load]]).
    ck = f"{serial}-{channel}"

    def _flag_keyerror():
        try:
            with open(keyerror_flag_path(serial, channel), "w") as fl:
                fl.write("1")
        except Exception:
            pass

    def _flag_offline():
        try:
            with open(offline_flag_path(serial, channel), "w") as fl:
                fl.write("1")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # ФОНОВАЯ VTM-сессия: один на воркер, а не на каждый запрос. Читатель
    # держит облачный поток открытым и наполняет лог; клиенты подключаются
    # к нему (см. [[_StreamSession]]). До этого каждый запрос открывал
    # новую VTM-сессию — handshake 1–3 с поверх ожидания I-кадра.
    # ------------------------------------------------------------------
    session = _StreamSession()

    def _open_session_once():
        """Открыть VTM-сессию, определить формат/codec.

        Возвращает (cm, pkts, buffered, transform, keyerr, annexb, codec);
        при неудаче None (флаги и инвалидация кеша уже применены)."""
        # "auto": сначала МАЛЫЙ поток, если кадр не придёт — основной ([[serve]]).
        # `set_substream` вызывается заново для каждой попытки, потому что он
        # подменяет формирователь URL библиотеки.
        attempts = [("sub", STREAM_PROBE_TIMEOUT), (mode, 20.0)] if mode == "auto" \
            else [(mode, 20.0)]
        last_err = None
        for want, tmo in attempts:
            is_probe = want == "sub" and mode == "auto"
            set_substream(want == "sub")
            try:
                cm = open_cloud_stream(client, serial, channel=channel,
                                       client_type=9, refresh_vtm=False, timeout=tmo)
            except Exception as e:
                last_err = e
                if is_probe:
                    continue                      # нет substream — пробуем основной
                break
            try:
                stream = cm.__enter__()
                stream.start()
                pkts = stream.iter_packets()
                buffered, is_rtp, codec = _prime_packets(pkts)
            except Exception as e:
                try:
                    cm.__exit__(None, None, None)
                except Exception:
                    pass
                last_err = e
                if is_probe:
                    continue                      # сокет молчит — к основному
                break
            if not buffered:
                try:
                    cm.__exit__(None, None, None)
                except Exception:
                    pass
                if is_probe:
                    continue
                last_err = last_err or RuntimeError("no packets from cloud")
                break
            # Формат/codec и декодер — на ВСЮ сессию (декодер состоянийый:
            # решение об inter-шифре, RTP seq-трекинг).
            if not is_rtp and buffered[0][:1] == b"\x47":
                annexb, codec = False, None       # MPEG-TS (без шифра)
                transform = lambda b: b
                keyerr = lambda: False
            elif not is_rtp:
                dec = PsStreamDecryptor(key, cache_key=ck)   # MPEG-PS (H.264/HEVC авто)
                annexb, codec = True, None
                transform = dec.feed; keyerr = lambda: dec.key_error
            elif codec == "h264":
                dec = H264RtpDecryptor(key, cache_key=ck)    # RTP/H.264
                annexb, codec = True, "h264"
                transform = dec.feed; keyerr = lambda: dec.key_error
            else:
                dec = HevcRtpDecryptor(key, cache_key=ck)    # RTP/HEVC
                annexb, codec = True, codec or "hevc"
                transform = dec.feed; keyerr = lambda: dec.key_error
            return cm, pkts, buffered, transform, keyerr, annexb, codec
        # Не открылось — старые правила (флаги / инвалидация кеша).
        # Офлайн-устройство НЕ СТИРАЕТ кеш — причина в [[_is_missing_resource]].
        e = str(last_err or "")
        if "offline" in e.lower() or "unreachable" in e.lower() \
                or _is_missing_resource(last_err):
            _flag_offline()
        else:
            vtm_cache.invalidate(_CACHE_KEY)   # метаданные могли устареть
        return None

    def _reader():
        """Читатель: держит VTM-сессию открытой и наполняет лог. При неудаче
        — перезапуск с backoff; клиенты замечают по смене gen."""
        backoff = 2.0
        while True:
            opened = _open_session_once()
            if opened is None:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            backoff = 2.0
            cm, pkts, buffered, transform, keyerr, annexb, codec = opened
            session.begin(annexb, codec)
            try:
                # Прайм-пакеты — тоже часть потока, отдаём их
                for b in buffered:
                    data = transform(b)
                    if data:
                        session.append(data)
                for pkt in pkts:
                    if keyerr():
                        _flag_keyerror()
                        break
                    data = transform(bytes(pkt.body))
                    if data:
                        session.append(data)
            except Exception as e:
                print("vtm session ended: %r" % (e,), file=sys.stderr, flush=True)
            finally:
                try:
                    cm.__exit__(None, None, None)
                except Exception:
                    pass
            time.sleep(min(backoff, 5.0))      # пауза между перезапусками

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            # Декодируем сырой поток и передаём напрямую (без внутреннего ffmpeg):
            #   RTP/PS -> Annex-B (H.264/HEVC), MPEG-TS -> как есть.
            # Внешний ffmpeg (stream_manager) определит codec auto-detect и
            # устойчиво декодирует.
            # Есть ли этот КАНАЛ среди облачных ресурсов? Даже если NVR online,
            # отсутствующий канал никогда не отдаст поток — не ждём 20s,
            # а возвращаем ТОЧНУЮ причину, а не "offline".
            if vtm_cache.channel_missing(_CACHE_KEY, serial, channel):
                _flag_offline()
                self.send_error(404, "channel not in cloud device list")
                return
            # Ждём готовности фоновой сессии (первые данные)
            deadline = time.time() + 20.0
            while True:
                with session.cond:
                    if session.seq > 0:
                        break
                if time.time() > deadline:
                    self.send_error(502, "stream not ready (cloud relay slow?)")
                    return
                time.sleep(0.2)

            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()

            # Запись в ОТДЕЛЬНОМ thread и с КОНТРОЛЕМ ([[PacedWriter]]) —
            # облачный reader никогда не блокируется, а 2-секундные
            # bursts распределяются по ровному потоку.
            writer = PacedWriter(self.wfile, session.codec,
                                 paced=session.annexb and PACE_FRAMES).start()
            gen, last = -1, 0
            try:
                while not writer.failed:
                    with session.cond:
                        session.cond.wait_for(
                            lambda: session.seq > last or session.gen != gen,
                            timeout=15.0)
                        if session.gen != gen:
                            gen = session.gen     # сессия перезапущена — новый снимок
                            last = 0
                        items = [d for s, d, _k in session.log if s > last]
                        last = session.seq
                    for it in items:
                        if writer.failed:
                            break
                        writer.feed(it)
                    # Поток молчит долго (сессия упала, читатель перезапускает)
                    # — закрываем; клиент (ffmpeg/VLC) переподключится.
                    if session.seq and time.time() - session.last_data > 30.0:
                        break
            finally:
                writer.close()

    threading.Thread(target=_reader, daemon=True).start()
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    try:
        server.serve_forever()
    finally:
        server.server_close()


KEY_ENV = "CLOUDCAM_CAM_KEY"   # то же самое, что stream_manager.KEY_ENV
PARENT_WATCH_ENV = "CLOUDCAM_PARENT_WATCH"   # то же самое, что stream_manager.PARENT_WATCH_ENV


def resolve_key(arg: str) -> str:
    """Если аргумент CODE равен "-", ключ берётся из окружения (чтобы не светился в argv)."""
    if arg != "-":
        return arg
    key = os.environ.get(KEY_ENV)
    if not key:
        sys.exit(f"Ключ не найден: CODE='-', но {KEY_ENV} пуст")
    return key


def watch_parent(stream=None, on_exit=None) -> threading.Thread:
    """Если родительский процесс умрёт, прокси тоже выйдет.

    stream_manager открывает прокси с stdin=PIPE и ничего в него не пишет.
    Когда родительский процесс завершится по любой причине (даже если его
    принудительно убьют), OS закроет pipe -> read() вернёт EOF. Без этого сиротский
    прокси держал бы порт занятым, и при следующем запуске этот порт был бы недоступен."""
    stream = stream if stream is not None else sys.stdin.buffer
    on_exit = on_exit or (lambda: os._exit(0))

    def run():
        try:
            while stream.read(4096):
                pass
        except Exception:
            pass
        on_exit()

    th = threading.Thread(target=run, daemon=True)
    th.start()
    return th


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Использование: python -m cloudcam.decrypt_proxy "
              "<SERIAL> <PORT> <CODE|-> [CHANNEL] [--stream=main|sub|auto]\n"
              f"  CODE='-' -> ключ берётся из переменной окружения {KEY_ENV}")
        sys.exit(1)
    if os.environ.get(PARENT_WATCH_ENV) == "1":
        watch_parent()
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = [a for a in sys.argv[1:] if a.startswith("--stream=")]
    mode = flags[-1].split("=", 1)[1] if flags else None
    if mode is None and "--substream" in sys.argv:
        mode = "sub"                    # старый флаг — обратная совместимость
    serve(args[0], int(args[1]), resolve_key(args[2]),
          int(args[3]) if len(args) > 3 else 1,
          stream_mode=mode)