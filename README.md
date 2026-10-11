# hikcloud2rtc — Hik-Connect NVR → локальный HTTP-сервис потока и снапшотов

Один контейнер `hikcloud2rtc`: логин в облако Hik-Connect, VTM-воркеры
расшифровки (HEVC/H.264, `cloudcam/decrypt_proxy`) и отдача потока и снапшотов
по HTTP. RTSP-агрегатор (go2rtc) в стеке нет — поток MPEG-TS по HTTP играют
VLC/ffplay.

## Проекты-основы

| Проект | Ссылка | Что взято |
|---|---|---|
| cloud-cam-viewer | https://github.com/Bahrombekk/cloud-cam-viewer | движок видео `cloudcam/decrypt_proxy`: VTM-сессия, RTP/PS/TS-автодетект, AES-128 по NAL (HEVC/H.264), правила сессии (featureCode, refresh) |
| hikcloudstream | https://github.com/cristianojmiranda/hikcloudstream | снапшот из облака (`/snapshot`, ~352×288) |
| pyezvizapi | https://github.com/RenierM26/pyEzvizApi | протокол облака (VTM relay, login/token) — зависимость `cloudcam/decrypt_proxy` |

## Запуск

```bash
cp .env.example .env        # вписать HIK_USER / HIK_PASSWORD
docker compose up -d --build
```

## Получение видео (поток)

Поток — **MPEG-TS, HEVC, по HTTP**. Параметры:
- `channel=N` — номер канала NVR (16 каналов; cam8 = `channel=8`)
- `stream=sub` — 640×360, ~0.2 Мбит/с
- `stream=main` — 1920×1080 (FullHD), ~1.75 Мбит/с

**ffplay (терминал):**
```bash
ffplay http://<host>:8000/stream.ts?channel=8&stream=sub
ffplay http://<host>:8000/stream.ts?channel=8&stream=main
```

**VLC:** Медиа → Открыть URL →
`http://<host>:8000/stream.ts?channel=8&stream=main`

**Записать 60 с:**
```bash
ffmpeg -i "http://<host>:8000/stream.ts?channel=8&stream=main" -t 60 -c copy rec.ts
```

Первые кадры появляются через 2–15 с (воркер открывает VTM-сессию и ждёт
ключевой кадр); дальше стрим тёплый.

## Получение снэпшота

**Из облака** — быстро (~1.7 с), фиксированное разрешение ~352×288:
```bash
curl -o cam8.jpg "http://<host>:8000/snapshot?channel=8"
```
(или просто открыть URL в браузере)

**FullHD-кадр из main-потока** — реальный декодированный кадр 1920×1080,
~6–11 с (ожидание ключевого кадра):
```bash
curl -o cam8_hd.jpg "http://<host>:8000/frame?channel=8&stream=main"
```

## API

| Эндпоинт | Что |
|---|---|
| `GET /health` | статус, движок, сессия |
| `GET /cameras` | список камер NVR |
| `GET /stream.ts?channel=N&stream=sub\|main` | живой MPEG-TS |
| `GET /snapshot?channel=N` | JPEG из облака (~352×288) |
| `GET /frame?channel=N&stream=main` | FullHD JPEG из потока (1920×1080) |

Другая камера — меняется `channel=`.

## Если нужен RTSP / HLS / WebRTC

Официальный go2rtc отдельным контейнером (в стек не входит):
```yaml
services:
  go2rtc:
    image: ghcr.io/alexxit/go2rtc:1.9.14
    ports: ["8554:8554", "1984:1984"]
    environment:
      GO2RTC_CONFIG_STREAMS: >
        cam8: "ffmpeg:http://<host>:8000/stream.ts?channel=8&stream=sub#video=copy"
        cam8_main: "ffmpeg:http://<host>:8000/stream.ts?channel=8&stream=main#video=copy"
```
Потом: `rtsp://<host>:8554/cam8`, `rtsp://<host>:8554/cam8_main`,
HLS `:8080/cam8/index.m3u8`, Web-плеер `:1984`.

## Задержка: PACE_FRAMES

Облако отдаёт кадры неровно: весь GOP приходит за один раз, затем пауза ~2 с.
`PacedWriter` в воркере сглаживает это, выдавая кадры ровным потоком.

- `PACE_FRAMES=1` (по умолчанию) — сглаживание: кадры выдаются ровным
  потоком, плавная картинка (глубина буфера ~1 с)
- `PACE_FRAMES=0` — кадры уходят сразу при получении: минимальная задержка,
  но 2-секундные застытия и «перемотка» в момент прихода GOP

Устанавливается в `.env`, перезапуск — `docker compose up -d`.

## Токены

`token.json` / `cam_keys.json` — в volume `data` (после деплоя
`hikcloud2rtc_data`), переживают перезапуск.
