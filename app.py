"""Hik-Connect cloud streaming service.

Video engine: cloudcam.decrypt_proxy (vendored from cloud-cam-viewer, MIT) —
H.264/HEVC over RTP/PS/TS with AES NAL decryption. The NVR here streams HEVC;
the original hikcloudstream pipeline is H.264-only and produced 0 bytes.
Snapshots: hikcloudstream cloud capture API (JPEG).

Architecture:
  GET /stream.ts?channel=N&stream=sub|main|auto
    -> one long-lived worker subprocess per (serial, channel, mode):
         python -m cloudcam.decrypt_proxy SERIAL PORT KEY CHANNEL --stream=M
       (private localhost port, opens VTM, decodes NAL, serves raw Annex-B/TS)
    -> service streams worker bytes; if Annex-B, remuxes to MPEG-TS (ffmpeg)
       so go2rtc's `ffmpeg:` source gets a native TS:
         rtsp://<host>:8554/camN

Env:
  HIK_USER, HIK_PASSWORD   Hik-Connect account (required)
  CHANNEL                  default NVR channel (0 = first active)
  STREAM                   default stream mode: sub|main|auto (default sub)
  PORT                     API port (default 8000)
  TOKEN_FILE               cloudcam session token (default /app/data/token.json)
  CAM_KEYS_FILE            camera keys, JSON {"SERIAL:CH": key} (default /app/data/cam_keys.json)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterator

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse

DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
TOKEN_FILE = os.environ.get("TOKEN_FILE", f"{DATA_DIR}/token.json")
CAM_KEYS_FILE = os.environ.get("CAM_KEYS_FILE", f"{DATA_DIR}/cam_keys.json")
# must be set BEFORE cloudcam import: settings caches active config at import time
os.makedirs(DATA_DIR, exist_ok=True)
os.environ["CLOUDCAM_TOKEN_FILE"] = TOKEN_FILE
os.environ["CLOUDCAM_CAMKEY_FILE"] = CAM_KEYS_FILE
os.environ.setdefault("CLOUDCAM_PLATFORM", "hikconnect")

import cloudcam.decrypt_proxy as dp            # install()s paged VTM + token cache
from cloudcam.client import CloudClient, MfaRequired

from hikcloudstream import Credentials, HikConnectClient

log = logging.getLogger("hiksvc")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")

DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
HIK_USER = os.environ.get("HIK_USER", "")
HIK_PASSWORD = os.environ.get("HIK_PASSWORD", "")
DEFAULT_CHANNEL = int(os.environ.get("CHANNEL", "0"))
DEFAULT_STREAM = os.environ.get("STREAM", "sub")
WORKER_PORT_BASE = int(os.environ.get("WORKER_PORT_BASE", "18100"))
_TOKEN_MAX_AGE = 1200.0      # refresh well before pyezvizapi 1500s limit
_CAM_LIST_TTL = 600.0

app = FastAPI(title="hikconnect-cloud-service")

# ---------------- cloudcam session ----------------

_cc_lock = threading.Lock()
_cc_client: CloudClient | None = None


def _cc() -> CloudClient:
    """cloudcam client with a live session (load token -> refresh -> login)."""
    global _cc_client
    with _cc_lock:
        if _cc_client is not None:
            age = time.time() - (_cc_client.session_time or 0)
            if age < _TOKEN_MAX_AGE:
                return _cc_client
        c = CloudClient(HIK_USER, HIK_PASSWORD, platform="hikconnect")
        if c.load_token(TOKEN_FILE) and not c.is_refresh_needed(max_age=_TOKEN_MAX_AGE):
            log.info("session resumed (age %.0fs)",
                     time.time() - (c.session_time or 0))
            _cc_client = c
            return c
        c.login()
        c.save_token(TOKEN_FILE)
        _cc_client = c
        log.info("cloudcam login ok, api_url=%s", c.api_url)
        return c


@app.get("/health")
def health():
    return {
        "status": "ok",
        "engine": "cloudcam.decrypt_proxy (H.264/HEVC, RTP/PS/TS)",
        "session": "live" if _cc_client else "not logged in yet",
        "cameras": "/cameras",
        "stream": f"/stream.ts?channel={DEFAULT_CHANNEL}&stream={DEFAULT_STREAM}",
        "stream_fullhd": f"/stream.ts?channel={DEFAULT_CHANNEL}&stream=main",
        "snapshot": f"/snapshot?channel={DEFAULT_CHANNEL}",
        "frame_fullhd": f"/frame?channel={DEFAULT_CHANNEL}&stream=main",
        "prime": f"/prime?channel={DEFAULT_CHANNEL}&stream=main",
        "worker_mode": WORKER_MODE,
        "workers_active": len([w for w in _workers.values() if w["proc"].poll() is None]),
        "rtsp": "rtsp://<host>:8554/cam%d (sub) / cam%d_main (FullHD) via go2rtc" % (DEFAULT_CHANNEL, DEFAULT_CHANNEL),
    }


@app.get("/workers")
def workers_debug():
    """Диагностика воркеров: активные клиенты и время простоя (для idle-timeout)."""
    now = time.time()
    out = []
    with _workers_lock:
        for key, w in _workers.items():
            out.append({
                "key": "%s ch=%d %s" % key,
                "alive": w["proc"].poll() is None,
                "active": w.get("active", 0),
                "reads": w.get("reads", 0),
                "idle_s": round(now - w["last_use"], 1),
                "timeout_s": WORKER_IDLE_TIMEOUT,
                "port": w["port"],
            })
    return {"mode": WORKER_MODE, "workers": out}


# ---------------- camera list ----------------

_CAM_LIST_TTL = 600.0
_CAM_LIST_TTL_EMPTY = 15.0   # пустой список — кэшируем коротко (облако может
                             # отваливаться временно, "Rpc invoke exception")

_cam_cache: tuple[float, list, float] | None = None


def _cameras(force: bool = False) -> list:
    """[(serial, channel, label)] — active VTM channels only.

    Облако Hik-Connect периодически отвечает на VTM pagelist ошибкой
    (meta.code 504 "Rpc invoke exception") — это временный сбой релея,
    не признак «камер нет». Поэтому пустой список ретраим и кэшируем
    с коротким TTL, чтобы /stream и /frame не ложились на «no active
    cameras in cloud» на 10 минут."""
    global _cam_cache
    now = time.time()
    if not force and _cam_cache and now - _cam_cache[0] < _cam_cache[2]:
        return _cam_cache[1]
    _cc()                       # ensure token file exists & fresh
    client = dp._make_client()
    cams = dp.list_cameras(client)
    if not cams:
        for _attempt in range(3):
            time.sleep(2)
            try:
                cams = dp.list_cameras(client)
            except Exception:  # noqa: BLE001
                pass
            if cams:
                break
    ttl = _CAM_LIST_TTL if cams else _CAM_LIST_TTL_EMPTY
    _cam_cache = (now, cams, ttl)
    log.info("camera list: %d active channels (ttl %.0fs)", len(cams), ttl)
    return cams


@app.get("/cameras")
def cameras(refresh: int = 0):
    try:
        cams = _cameras(force=bool(refresh))
    except MfaRequired:
        raise HTTPException(503, "2FA required (see /keys/request)")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"camera list failed: {e}") from e
    return [{"serial": s, "channel": ch, "name": label} for s, ch, label in cams]


def _resolve(channel: int, serial: str | None):
    cams = _cameras()
    if not cams:
        raise HTTPException(502, "no active cameras in cloud")
    if serial:
        hit = [c for c in cams if c[0].upper() == serial.upper()]
        if not hit:
            raise HTTPException(404, f"device {serial} not in cloud list")
        if channel > 0:
            hit = [c for c in hit if c[1] == channel] or hit
        return hit[0]
    ch = channel if channel > 0 else (DEFAULT_CHANNEL or cams[0][1])
    for c in cams:
        if c[1] == ch:
            return c
    raise HTTPException(404, f"channel {ch} not active; available: "
                             f"{sorted(set(c[1] for c in cams))}")


# ---------------- camera keys (encrypted cameras only) ----------------

def _save_key(serial: str, channel: int, key: str) -> None:
    kk = {}
    try:
        kk = json.load(open(CAM_KEYS_FILE, encoding="utf-8"))
    except (OSError, ValueError):
        pass
    kk[f"{serial}:{channel}"] = str(key)
    tmp = CAM_KEYS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(kk, f)
    os.replace(tmp, CAM_KEYS_FILE)


@app.post("/keys/request")
def keys_request(serial: str, channel: int = Query(DEFAULT_CHANNEL)):
    """Request the camera media key; cloud may demand a 2FA code by email."""
    c = _cc()
    try:
        key = c.get_cam_key(serial, channel)
        _save_key(serial, channel, key)
        _restart_worker(serial, channel)
        return {"ok": True, "stored": True}
    except MfaRequired:
        c.send_mfa_code()
        return {"ok": True, "mfa": "code sent to account email; "
                                   "POST /keys/set {serial, channel, code}"}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"key request failed: {e}") from e


@app.post("/keys/set")
def keys_set(serial: str, channel: int = Query(DEFAULT_CHANNEL), code: str = ""):
    c = _cc()
    try:
        key = c.get_cam_key(serial, channel, mfa_code=code or None)
    except MfaRequired:
        raise HTTPException(400, "2FA code missing/invalid")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"key set failed: {e}") from e
    _save_key(serial, channel, key)
    _restart_worker(serial, channel)
    return {"ok": True, "stored": True}


def _restart_worker(serial: str, channel: int) -> None:
    with _workers_lock:
        for k, w in list(_workers.items()):
            if k[0] == serial and k[1] == channel:
                w["proc"].kill()
                del _workers[k]


# ---------------- stream workers ----------------

# WORKER_MODE: always = воркеры живут 24/7 (тёплый поток, трафик с облака
# постоянный); idle = воркер убивается через WORKER_IDLE_TIMEOUT секунд без
# клиентов — трафик только во время опросов (сценарий «звонок в домофон»).
WORKER_MODE = os.environ.get("WORKER_MODE", "always").strip().lower()
if WORKER_MODE not in ("always", "idle"):
    WORKER_MODE = "always"
WORKER_IDLE_TIMEOUT = float(os.environ.get("WORKER_IDLE_TIMEOUT", "90"))

_workers: dict[tuple, dict] = {}
_workers_lock = threading.RLock()
_next_port = {"i": 0}


def _worker_port() -> int:
    with _workers_lock:
        _next_port["i"] += 1
        return WORKER_PORT_BASE + (_next_port["i"] - 1) % 400


def _cam_key(serial: str, channel: int) -> str:
    try:
        kk = json.load(open(CAM_KEYS_FILE, encoding="utf-8"))
    except (OSError, ValueError):
        return "0" * 32
    return str(kk.get(f"{serial}:{channel}") or kk.get(serial) or "0" * 32)


def _ensure_worker(serial: str, channel: int, mode: str) -> int:
    key = (serial, channel, mode)
    with _workers_lock:
        w = _workers.get(key)
        if w and w["proc"].poll() is None:
            w["last_use"] = time.time()
            return w["port"]
        port = _worker_port()
        env = dict(os.environ,
                   CLOUDCAM_TOKEN_FILE=TOKEN_FILE,
                   CLOUDCAM_CAM_KEY=_cam_key(serial, channel))
        cmd = [sys.executable, "-m", "cloudcam.decrypt_proxy",
               serial, str(port), "-", str(channel), f"--stream={mode}"]
        # stdin=PIPE + CLOUDCAM_PARENT_WATCH=1: если процесс сервиса умрёт
        # (не перезапустив контейнер), воркер закроется вместе с ним и не
        # оставит сирот, державших порты и VTM-сессии ([[watch_parent]]).
        env["CLOUDCAM_PARENT_WATCH"] = "1"
        try:
            proc = subprocess.Popen(
                cmd, cwd=os.path.dirname(os.path.abspath(__file__)), env=env,
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                stderr=open(f"/tmp/worker_{serial}_{channel}_{mode}.log", "ab", buffering=0))
        except Exception as e:  # noqa: BLE001
            raise HTTPException(502, f"worker start failed: {e}") from e
        _workers[key] = {"proc": proc, "port": port,
                         "last_use": time.time(), "active": 0}
        log.info("worker %s ch=%d %s -> :%d (pid %d)", serial, channel, mode, port, proc.pid)
        return port


def _worker_touch(port: int) -> None:
    """Клиент подключился к воркеру: отметим активность (для idle-timeout)."""
    with _workers_lock:
        for w in _workers.values():
            if w["port"] == port and w["proc"].poll() is None:
                w["last_use"] = time.time()
                w["active"] += 1
                return


def _worker_untouch(port: int) -> None:
    with _workers_lock:
        for w in _workers.values():
            if w["port"] == port and w["proc"].poll() is None:
                w["active"] = max(0, w["active"] - 1)
                w["last_use"] = time.time()
                return


def _reap_idle_workers() -> None:
    """WORKER_MODE=idle: убиваем воркеры без клиентов дольше WORKER_IDLE_TIMEOUT.

    Трафик с облака тогда идёт только вокруг опросов (звонок в домофон →
    /prime → /frame → через 90 с тишины воркер и VTM-сессия закрываются).
    """
    if WORKER_MODE != "idle":
        return
    now = time.time()
    with _workers_lock:
        for key, w in list(_workers.items()):
            alive = w["proc"].poll() is None
            age = now - w["last_use"]
            # диагностика: логим состояние каждого воркера, пока режим idle
            log.debug("reaper: worker %s alive=%s active=%d idle=%.0fs (timeout %.0fs)",
                      key, alive, w.get("active", 0), age, WORKER_IDLE_TIMEOUT)
            if alive and w.get("active", 0) == 0 and age > WORKER_IDLE_TIMEOUT:
                log.info("worker %s idle %.0fs — stop", key, age)
                try:
                    w["proc"].kill()
                except ProcessLookupError:
                    pass
                del _workers[key]


def _reaper_loop() -> None:
    while True:
        time.sleep(15)
        try:
            _reap_idle_workers()
        except Exception as e:  # noqa: BLE001
            log.warning("reaper: %s", e)


threading.Thread(target=_reaper_loop, daemon=True, name="worker-reaper").start()
log.info("worker mode: %s (idle timeout %.0fs)", WORKER_MODE, WORKER_IDLE_TIMEOUT)


def _wait_port(port: int, timeout: float = 8.0) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.25)
    return False


def _worker_open(port: int, retries: int = 3, pause: float = 3.0):
    """Открыть HTTP-поток воркера, с ретраями на временные сбои облака.

    Воркер отвечает 502, когда VTM-сессия не открылась: релея облака
    периодически отваливается (504 «Rpc invoke exception») на десятки секунд,
    устройство уходит в оффлайн и т.п. Без ретраев короткий сбой облака
    превращался в ошибку для клиента; теперь переспрашиваем несколько раз
    (каждый запрос к воркеру — новая попытка открыть VTM) и лишь затем
    отдаём честную 502, а не 500."""
    last = None
    for attempt in range(1, retries + 1):
        req = urllib.request.Request("http://127.0.0.1:%d/" % port)
        try:
            r = urllib.request.urlopen(req, timeout=40)
            _worker_touch(port)
            return r
        except urllib.error.HTTPError as e:
            last = e
            log.warning("worker :%d http %s (attempt %d/%d)",
                        port, e.code, attempt, retries)
        except (OSError, urllib.error.URLError) as e:
            last = e
            log.warning("worker :%d unreachable: %s (attempt %d/%d)",
                        port, e, attempt, retries)
        time.sleep(pause)
    raise HTTPException(502,
                        "stream worker (:%d) failed to open stream: %s" % (port, last))


def _worker_stream(r, port: int = 0) -> Iterator[bytes]:
    """Worker bytes as soon as they arrive (read1 — no full-buffer waits)."""
    try:
        while True:
            b = r.read1(65536)
            if not b:
                break
            if port:
                with _workers_lock:
                    for w in _workers.values():
                        if w["port"] == port:
                            w["reads"] = w.get("reads", 0) + 1
                            break
            yield b
    except Exception as e:  # noqa: BLE001
        log.info("worker stream ended: %s", e)
    finally:
        try:
            r.close()
        except Exception:
            pass
        if port:
            _worker_untouch(port)


def _ts_remux(source, codec_guess: str) -> Iterator[bytes]:
    """Annex-B -> MPEG-TS (go2rtc `ffmpeg:` source wants TS).

    source = (head: bytes, raw: iterator). The remux feeds ffmpeg from a
    background thread (avoids stdin/stdout pipe deadlock) while this generator
    yields remuxed TS to the client. The feed thread OWNS the source's cleanup:
    when ffmpeg dies (client disconnect) the feed breaks and closes `raw`,
    which runs _worker_stream's finally (r.close + _worker_untouch). Without
    that close the active-client counter leaks and WORKER_MODE=idle never
    reaps the worker.
    """
    head, raw = source
    p = subprocess.Popen(
        ["ffmpeg", "-hide_banner", "-loglevel", "error",
         # low-latency: default input probe (analyzeduration 5 s / probesize
         # 5 MB) held the first TS packet ~2.3 s even though the format and
         # codec are known. Small probe = first packet passes in ~0.1 s.
         "-probesize", "32768", "-analyzeduration", "100",
         "-f", codec_guess, "-i", "pipe:0",
         "-c", "copy", "-f", "mpegts", "pipe:1"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    assert p.stdin and p.stdout

    def _feed():
        try:
            p.stdin.write(head)
            for chunk in raw:
                # ffmpeg мёртв (клиент отвалился) — не читаем источник вечно
                if p.poll() is not None:
                    break
                p.stdin.write(chunk)
        except Exception:
            pass
        finally:
            try:
                p.stdin.close()
            except Exception:
                pass
            # закрываем источник: доходит _worker_stream finally →
            # r.close() + _worker_untouch (иначе idle-timeout не сработает)
            try:
                raw.close()
            except Exception:
                pass

    threading.Thread(target=_feed, daemon=True).start()
    try:
        while True:
            # read1: return as soon as ANY bytes are available. read(131600)
            # blocked until 128 KB accumulated — at sub bitrate (~25 KB/s) that
            # is ~5 s of constant delay behind the camera (measured).
            out = p.stdout.read1(131072)
            if not out:
                break
            yield out
    finally:
        if p.poll() is None:
            p.kill()


def _stream_gen(r, port: int = 0) -> Iterator[bytes]:
    """Detect format from the FIRST chunk (~100 ms) and stream immediately.

    No large pre-buffer: go2rtc pulls ffmpeg with `-fflags nobuffer` and aborts
    ("Immediate exit") if the first TS packet is held back for seconds.
    """
    raw = _worker_stream(r, port)
    try:
        head = next(raw)
    except StopIteration:
        return
    if not head:
        return

    if head[:1] == b"\x47":
        # Worker already emits MPEG-TS — pass through untouched.
        yield head
        yield from raw
        return

    # Annex-B: detect codec from the first NAL, then remux to MPEG-TS.
    codec = _detect_annexb_codec(head)
    log.info("worker :%d emits Annex-B (%s) -> remux MPEG-TS (first chunk %dB)",
             port, codec, len(head))
    yield from _ts_remux((head, raw), codec)


def _detect_annexb_codec(head: bytes) -> str:
    """H.264 vs HEVC from the first NAL header in the head."""
    idx = head.find(b"\x00\x00\x00\x01")
    if idx == -1:
        idx = head.find(b"\x00\x00\x01")
    if idx != -1 and idx + 3 < len(head):
        b0 = head[idx + 4 if idx + 4 <= len(head) else idx + 3]
        # HEVC NAL types: VPS32/SPS33/PPS34/IDR..36; H.264: SPS7/PPS8/IDR5
        htype = (b0 >> 1) & 0x3F
        if htype in (19, 20, 21, 16, 17, 18, 32, 33, 34, 35, 36):
            return "hevc"
        if (b0 & 0x1F) in (1, 5, 6, 7, 8, 9):
            return "h264"
    return "hevc"   # this NVR fleet streams HEVC




@app.get("/stream.ts")
async def stream_ts(request: Request,
                    channel: int = Query(DEFAULT_CHANNEL),
                    serial: str = Query(""),
                    stream: str = Query(DEFAULT_STREAM)):
    if stream not in ("sub", "main", "auto"):
        stream = "sub"
    try:
        s, ch, _label = _resolve(channel, serial or None)
    except HTTPException:
        raise
    port = _ensure_worker(s, ch, stream)
    if not _wait_port(port, 8.0):
        raise HTTPException(502, f"stream worker (:{port}) did not start")
    r = _worker_open(port)  # ретраи на временный сбой VTM-облака (502 воркера)

    q: "queue.Queue" = queue.Queue()

    def _pump():
        # Один владелец генератора: перебираем его в этом трее и кладём
        # чанки в очередь. finally (gen.close()) — детерминированная точка
        # очистки: _ts_remux finally (убийство ffmpeg) + raw.close() →
        # _worker_stream finally (r.close + _worker_untouch).
        gen = _stream_gen(r, port)
        try:
            for chunk in gen:
                q.put(chunk)
        except Exception as e:  # noqa: BLE001
            log.info("stream.ts pump ended: %s", e)
        finally:
            try:
                gen.close()
            except Exception:
                pass
            q.put(None)  # EOF-маркер для async-покупателя

    pump = threading.Thread(target=_pump, daemon=True, name="stream-pump")
    pump.start()

    async def _agen():
        try:
            while True:
                if await request.is_disconnected():
                    log.info("stream.ts client disconnected (:%d)", port)
                    return
                try:
                    item = await asyncio.wait_for(
                        asyncio.get_running_loop().run_in_executor(
                            None, q.get), 2.0)
                except asyncio.TimeoutError:
                    continue  # пауза в потоке (между GOP) — проверяем disconnect
                if item is None:
                    return
                yield item
        finally:
            # Клиент ушёл (или поток закончился): закрываем генератор.
            # gen.close() из async-контекста не безопасно, но мы НЕ
            # перебираем его параллельно: pump уже остановился (EOF) либо
            # блокируется на read — close() из другого треда для генератора
            # в состоянии suspended-safe только через GC. Поэтому здесь
            # закрываем то, что принадлежит НАМ: pump-тред сам завершит
            # gen.close() в своём finally, когда заметит закрытый r /
            # умерший ffmpeg. Для надёжности в случае зависшего pump —
            # принудительный r.close() (read1 упадёт → pump выйдет →
            # его finally закроет gen и сотрёт active-счётчик).
            try:
                r.close()
            except Exception:
                pass
            pump.join(timeout=3.0)

    return StreamingResponse(_agen(), media_type="video/mp2t",
                             headers={"Cache-Control": "no-store"})


@app.get("/prime")
def prime(channel: int = Query(DEFAULT_CHANNEL),
          serial: str = Query(""),
          stream: str = Query("main")):
    """Прогрев воркера по триггеру (звонок в домофон).

    Запускает воркер и ждёт, пока фоновая VTM-сессия начнёт давать данные,
    затем закрывает соединение — сессия в воркере остаётся ТЕПЛОЙ, и
    следующий /frame отвечает за ~4 с (GOP), а не 6–8 с холодного старта.
    В WORKER_MODE=always это избыточно (воркер и так живёт 24/7), в idle —
    вот он и есть «разбуживание».

    GET /prime?channel=8&stream=main  →  {"warm": true, "waited_s": 2.4}
    """
    if stream not in ("sub", "main"):
        stream = "main"
    try:
        s, ch, _label = _resolve(channel, serial or None)
    except HTTPException:
        raise
    t0 = time.time()
    port = _ensure_worker(s, ch, stream)
    if not _wait_port(port, 8.0):
        raise HTTPException(502, f"stream worker (:{port}) did not start")
    # Ответ 200 от воркера = сессия уже отдаёт данные (см. decrypt_proxy do_GET)
    try:
        r = _worker_open(port)
    except HTTPException:
        raise
    _worker_untouch(port)
    try:
        r.close()
    except Exception:
        pass
    return {"warm": True, "waited_s": round(time.time() - t0, 2),
            "worker_mode": WORKER_MODE}


@app.get("/frame")
def frame(channel: int = Query(DEFAULT_CHANNEL),
          serial: str = Query(""),
          stream: str = Query("main"),
          max_wait: int = Query(60)):
    """Full-resolution JPEG: decodes ONE frame from the live stream (main = 1080p).

    Unlike /snapshot (cloud API, fixed low resolution), the image is a real
    decoded frame at the stream's native resolution. We decode the first
    INTRA (I) frame: a P-frame from mid-GOP has no reference in a fresh
    decoder buffer and renders as a corrupted block of garbage."""
    if stream not in ("sub", "main"):
        stream = "main"
    try:
        s, ch, _label = _resolve(channel, serial or None)
    except HTTPException:
        raise
    port = _ensure_worker(s, ch, stream)
    if not _wait_port(port, 8.0):
        raise HTTPException(502, f"stream worker (:{port}) did not start")

    out = Path(tempfile.mktemp(prefix="frame_", suffix=".jpg"))
    # Сбой VTM-облака (502 воркера) или обрыв потока до первого I-кадра —
    # частый случай: повторяем «открыть + дождаться I-кадра» пару раз.
    try:
        for attempt in (1, 2):
            r = _worker_open(port)  # ретраи на временный сбой VTM-облака
            # Stream the live pipe straight into ffmpeg and pick the first
            # I-frame (IDR). The worker is persistent, so a capture can start
            # mid-GOP; decoding a P-frame there gives half-corrupted frames.
            # The comma inside eq() must be quoted — it would otherwise be
            # read as a filter-graph separator.
            dec = subprocess.Popen(
                ["ffmpeg", "-hide_banner", "-loglevel", "error",
                 "-f", "mpegts", "-i", "pipe:0",
                 "-vf", "select='eq(pict_type,I)'",
                 "-frames:v", "1", "-q:v", "2", "-f", "image2", str(out)],
                stdin=subprocess.PIPE, stderr=subprocess.DEVNULL)
            st = time.time()
            gen = _stream_gen(r, port)
            try:
                for chunk in gen:
                    try:
                        dec.stdin.write(chunk)
                    except (BrokenPipeError, ConnectionResetError):
                        break  # ffmpeg found the I-frame and closed stdin
                    if time.time() - st >= max_wait:
                        break
            finally:
                # gen.close() детерминированно доводит генератор до finally
                # (_worker_stream → _worker_untouch): иначе break из for-цикла
                # оставил бы счётчик активных клиентов >0 навсегда и
                # idle-timeout в WORKER_MODE=idle бы не сработал.
                try:
                    gen.close()
                except Exception:
                    pass
                try:
                    dec.stdin.close()
                except (BrokenPipeError, OSError):
                    pass  # ffmpeg already exited after the I-frame
                if dec.poll() is None:
                    try:
                        dec.kill()  # timeout or capture aborted — don't leak ffmpeg
                    except ProcessLookupError:
                        pass
                    try:
                        dec.wait(timeout=max_wait)
                    except subprocess.TimeoutExpired:
                        pass
            if out.exists() and out.stat().st_size:
                jpeg = out.read_bytes()
                break
            if attempt == 2:
                raise HTTPException(504, "no keyframe (I-frame) in stream window")
            time.sleep(3.0)  # облако может восстановиться — повторяем
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "decode timeout")
    finally:
        out.unlink(missing_ok=True)
    log.info("frame %s ch=%d %s -> %d bytes", s, ch, stream, len(jpeg))
    return Response(jpeg, media_type="image/jpeg")


# ---------------- snapshot (hikcloudstream) ----------------

_snap_lock = threading.Lock()
_snap_client: HikConnectClient | None = None
_snap_cams: list = []


def _snap_login() -> None:
    """(Пере)логин в hikcloudstream и перечитать список камер."""
    if not HIK_USER or not HIK_PASSWORD:
        raise HTTPException(502, "HIK_USER/HIK_PASSWORD not set")
    global _snap_client, _snap_cams
    client = HikConnectClient()
    client.login(Credentials(HIK_USER, HIK_PASSWORD))
    cams = client.list_cameras()
    _snap_client = client
    _snap_cams = cams
    log.info("hikcloudstream login ok, %d cameras", len(cams))


def _snap() -> HikConnectClient:
    global _snap_client, _snap_cams
    with _snap_lock:
        if _snap_client is None:
            _snap_login()
        return _snap_client


def _is_unauthorized(e: Exception) -> bool:
    s = str(e)
    return "401" in s or "Unauthorized" in s or "unauthorized" in s


@app.get("/snapshot")
def snapshot(channel: int = Query(DEFAULT_CHANNEL)):
    try:
        c = _snap()
        if channel <= 0:
            cam = _snap_cams[0]
        else:
            cam = next((x for x in _snap_cams if x.channel_no == channel), None)
            if cam is None:
                raise HTTPException(404, f"channel {channel} not found; "
                                         f"available: {[x.channel_no for x in _snap_cams]}")
        try:
            data = c.capture_snapshot(cam)
        except Exception as e:  # noqa: BLE001
            # 401 — токен снапшот-клиента истёк (он логируется один раз на
            # весь аптайм): перезалогиниваемся и повторяем один раз.
            if not _is_unauthorized(e):
                raise
            with _snap_lock:
                _snap_login()
            c2 = _snap_client
            cam = next((x for x in _snap_cams if x.channel_no == channel),
                       _snap_cams[0]) if channel > 0 else _snap_cams[0]
            if c2 is None:
                raise HTTPException(502, "snapshot relogin failed")
            data = c2.capture_snapshot(cam)
        return Response(data, media_type="image/jpeg")
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"snapshot failed: {e}") from e


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
