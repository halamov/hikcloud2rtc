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
| `GET /preview?cell=320&workers=8&quality=88` | сетка снапшотов всех камер (авто-раскладка, NO SIGNAL для недоступных) |
| `GET /prime?channel=N&stream=main` | прогреть воркер заранее (тёплый старт) |
| `GET /workers` | диагностика: воркеры, активные клиенты, простой (idle-timeout) |
| `GET /logs?tail=N` | хвост app.log; `?worker=worker_<serial>_<ch>_<mode>` — лог воркера |

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

## Режим воркеров: WORKER_MODE

Внутри воркера (процесс на канал) работает **фоновая VTM-сессия**: облачный
поток держится открытым постоянно, данные складываются в кольцо (хвост от
последнего keyframe). Подключение `/stream.ts` или `/frame` к тёплому
воркеру — мгновенное; `/frame` ждёт только следующий I-кадр (GOP, ~4 с).
За это платим трафиком: пока воркер жив, облако гоняет ему поток 24/7
(замер: main ~1.7 Мбит/с ≈ 18 ГБ/день, sub ~0.11 Мбит/с ≈ 1.2 ГБ/день).

Два режима (`.env` → `WORKER_MODE`):

- **`always`** (по умолчанию) — воркеры живут 24/7. Любой запрос тёплый,
  минимальная задержка. Цена — постоянный трафик с облака и VTM-сессия на
  каждую используемую камеру.
- **`idle`** — воркер убивается через `WORKER_IDLE_TIMEOUT` (по умолчанию
  90 с) без клиентов: трафик только вокруг опросов. Сценарий
  «звонок в домофон → распознание → открыть»:

  ```
  триггер звонка → GET /prime?channel=8&stream=main   # ~1–3 с, воркер грейтся
               → GET /frame?channel=8&stream=main     # ~4 с, FullHD JPEG
  ... распознавание ...
  (90 с тишины → воркер и VTM-сессия закрыты)
  ```

  Без `/prime` первый `/frame` после простоя — холодный старт (~6–8 с).
  `/prime` нужен только в режиме `idle`; в `always` воркер и так тёплый.

Выбор: камеры смотрят постоянно (VLC, дашборд) → `always`; опрос по
событию (до 1–2 раз в сутки) → `idle`.

## Токены

`token.json` / `cam_keys.json` — в volume `data` (после деплоя
`hikcloud2rtc_data`), переживают перезапуск.

## Логи

Три уровня:

- **app.log** — сервис (запросы, воркеры, reaper, 502-ретраи). Файл
  `LOG_DIR/app.log` (по умолчанию `/app/data/logs`, т.е. в volume `data` —
  переживает деплой), ротация 5×5 МБ. Дублируется в console → `docker logs`.
- **`worker_<serial>_<ch>_<mode>.log`** — stderr каждого воркера
  (VTM-сессия, обрывы). В том же `LOG_DIR`, переживает деплой.
- **docker json-file** — console-лог контейнера, ограничен
  `max-size: 10m, max-file: 3` (составная часть compose).

Смотреть:

```bash
# из контейнера / на хосте
docker logs hikcloud2rtc --tail 100 -f
# через API (без доступа к хосту)
curl "http://<host>:8000/logs?tail=50"
curl "http://<host>:8000/logs?worker=worker_J88212190_8_main&tail=20"
```
