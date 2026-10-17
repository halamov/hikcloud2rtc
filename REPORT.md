# Отчёт: hikcloud2rtc

Презентация проекта: `hikcloud2rtc.pptx` (рядом с этим файлом).

Hik-Connect NVR → HTTP-сервис видео-потока и снапшотов. Python/FastAPI + cloudcam (декодирование потока) + ffmpeg.

## Что сделано (по git-логу)

| Коммит | Что |
|---|---|
| `f91a186` | Базовый сервис: Hik-Connect NVR → HTTP (поток и снапшоты), FastAPI, cloudcam/decrypt_proxy |
| `4a1a2ad` | frame: декодирование первого I-кадра (IDR) — убрало «половинчатые» кадры посреди GOP |
| `e77e871` | frame: глотание BrokenPipeError при закрытии stdin после выхода ffmpeg |
| `14d2449` | vtm_cache: облачные ошибки не кэшируются; app: retry/короткий TTL на пустой список |
| `a53c68f` | Ретраи открытия потока + честная 502 вместо 500 |
| `ecb076c` | Воркер: фоновая VTM-сессия (тёплые фреймы) + оптимизации A/B/C/D |
| `cd943dd` | `WORKER_MODE=always\|idle` + `WORKER_IDLE_TIMEOUT` + `/prime`; idle-reaper — фикс утечки активных клиентов при обрыве /frame и /stream.ts |
| `eb3a8c5` | Документация: endpoint `/workers` + WORKER_MODE в README |
| `04a5d85` | snapshot: re-login + 1 retry при 401 (токен hikcloudstream истёк; клиент жил «один раз на аптайм») |
| `f4bd514` | Логи: `LOG_DIR` (volume) + RotatingFileHandler app.log (5×5 МБ), воркер-логи в volume (были в /tmp — сгорали при деплое), docker json-file max-size 10m×3, эндпоинт `/logs` (хвост app.log / воркер-лога без доступа к хосту) |

## Ключевые решения
- **Рабочая модель воркера:** фоновая VTM-сессия держит поток открытым постоянно (режим `always`) или с тайм-аутом в простое (`idle` + `/prime` для «прогрева»). Без воркера каждый запрос к потоку переподключался с нуля.
- **IDR-кадры:** первый кадр всегда берётся с I-кадра, иначе декодер выдавал артефакты.
- **Честные ошибки:** облако NVR недоступно → 502 с ретраями, а не 500 и не кэш мусора.
- **Утечка клиентов:** при обрыве клиента ffmpeg-пайплайны и VTM-сессии собирались idle-reaper'ом.

## Деплой
- Живёт на home-nuc2 (env 4, Dockhand, t.hals.home:9002) как git-стек с GitHub `halamov/hikcloud2rtc`, деплой через Dockhand API.
- Старый хост 192.168.201.70 (alias `hik`) погашен 16.10.2026, каталог и volume там остались как fallback.
