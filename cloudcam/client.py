"""
Облачный клиент EZVIZ / Hik-Connect.

Подключается как Android-приложение (обходит terminal limit, code 1069).

Управление сессией опирается на три правила — все они проверены в других
проектах (`ezviz-ha-addon`, `hikconnect`, `pyezvizapi`):

  1. `featureCode` привязан к ЭТОЙ УСТАНОВКЕ ([[identity]]) — а не к жёстко
     записанному общему коду. Иначе каждая копия проекта — ОДИН терминал.
  2. При каждом запуске НЕ ВЫПОЛНЯЕТСЯ ПОВТОРНЫЙ LOGIN. Сохранённый token
     проверяется и при необходимости обновляется (refresh); login — крайняя
     мера. Каждый login в облаке вызывает CAPTCHA (1015) и 2FA «новое
     устройство».
  3. При неудачном login повторных попыток нет — пауза растёт с 60s
     до 30 минут.
"""

import hashlib
import json
import time

import requests

from .identity import feature_code


def md5(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()


class MfaRequired(Exception):
    """Повышается, когда для получения ключа нужен код 2FA (email)."""


class CloudClient:
    """Вход в облако EZVIZ / Hik-Connect и список устройств."""

    # Параметры платформы: clientType и начальный домен
    PLATFORMS = {
        "hikconnect": {"client_type": "55", "domain": "api.hik-connect.com", "lang": "en-US"},
        "ezviz":      {"client_type": "1",  "domain": "apiisgp.ezvizlife.com", "lang": "en_US"},
    }

    # Сессия обновляется, если её возраст превышает столько секунд. Библиотека
    # `hikconnect` идёт тем же путём (`is_refresh_login_needed`): в отличие от
    # периодического таймера «каждый час», обновление по ВОЗРАСТУ
    # работает корректно даже после повторного подключения.
    SESSION_MAX_AGE = 1500.0
    # Пауза при неудачном login: 60s -> удвоение -> 30 минут.
    LOGIN_BACKOFF_MIN = 60.0
    LOGIN_BACKOFF_MAX = 1800.0

    def __init__(self, email, password, region=None, platform="hikconnect"):
        self.email = email
        self.password = password
        self.platform = platform if platform in self.PLATFORMS else "hikconnect"
        cfg = self.PLATFORMS[self.platform]
        # если region задан — используем его, иначе стандарт платформы
        self.api_url = region or cfg["domain"]
        self.session_id = None
        self.rf_session_id = None
        self.user_code = None
        self.session_time = 0.0      # когда получена сессия (epoch) — также пишется в файл
        self._login_fail_at = 0.0    # последнее неудачное login
        self._login_backoff = 0.0    # ожидание до следующей попытки

        self.session = requests.Session()
        self.session.headers.update(self._base_headers())

    @property
    def FEATURE_CODE(self) -> str:      # noqa: N802 — старое имя сохраняется
        """Терминальный код этой установки ([[identity.feature_code]])."""
        return feature_code()

    def _base_headers(self) -> dict:
        """Полные заголовки Android-приложения.

        Раньше отправлялись только 4. Некоторые endpoint'ы облака
        (особенно `devconfig/*` — код подтверждения берётся отсюда) без полного
        набора отвечают пустым или обрезанным ответом."""
        cfg = self.PLATFORMS[self.platform]
        return {
            "clientType": cfg["client_type"],
            "lang": cfg["lang"],
            "language": cfg["lang"],
            "featureCode": self.FEATURE_CODE,
            "netType": "WIFI",
            "customno": "1000001",
            "clientNo": "web_site",
            "appId": "ys7",
            "osVersion": "",
            "clientVersion": "",
            "ssid": "",
            "User-Agent": "okhttp/3.12.1",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def login(self):
        """Вход в аккаунт. API входа Hik-Connect и EZVIZ различаются.

        При серии неудач ожидание растёт ([[LOGIN_BACKOFF_MIN]]):
        облако блокирует частые попытки с неверным паролем и требует CAPTCHA,
        а цикл 24/7 иначе опрашивал бы раз в секунду."""
        wait = self._login_fail_at + self._login_backoff - time.time()
        if wait > 0:
            raise RuntimeError(
                f"Login повторится через {wait:.0f}s "
                f"(предыдущая попытка не удалась)")
        try:
            data = (self._login_ezviz() if self.platform == "ezviz"
                    else self._login_hikconnect())
        except Exception:
            self._login_fail_at = time.time()
            self._login_backoff = min(
                self.LOGIN_BACKOFF_MAX,
                max(self.LOGIN_BACKOFF_MIN, self._login_backoff * 2))
            raise
        self._login_fail_at = 0.0
        self._login_backoff = 0.0
        return data

    def _login_hikconnect(self):
        """Hik-Connect: endpoint /v2, глобальный домен, redirect по region (1100)."""
        data = None
        for _ in range(4):
            r = self.session.post(
                f"https://{self.api_url}/v3/users/login/v2",
                data={
                    "account": self.email,
                    "password": md5(self.password),
                    "featureCode": self.FEATURE_CODE,
                },
                timeout=25,
            )
            data = r.json()
            code = data.get("meta", {}).get("code")
            if code == 1100:  # перенаправление на другой region
                self.api_url = data["loginArea"]["apiDomain"]
                continue
            if code in (1013, 1014, 1226):
                raise RuntimeError("Ошибка логина: неправильный email или пароль")
            if code == 1015:
                raise RuntimeError(
                    "Запрошен CAPTCHA — сначала войдите в приложение Hik-Connect, затем повторите")
            break
        return self._finish_login(data)

    def _login_ezviz(self):
        """EZVIZ: endpoint /v5, clientType 1 (обходит terminal limit)."""
        r = self.session.post(
            f"https://{self.api_url}/v3/users/login/v5",
            data={
                "account": self.email,
                "password": md5(self.password),
                "featureCode": self.FEATURE_CODE,
                "msgType": 0,
                "cuName": "SSmartPhone_Android",
            },
            timeout=25,
        )
        return self._finish_login(r.json())

    def _finish_login(self, data):
        if not data or data.get("meta", {}).get("code") != 200:
            raise RuntimeError(f"Ошибка логина: {data.get('meta') if data else 'нет ответа'}")
        self.session_id = data["loginSession"]["sessionId"]
        self.rf_session_id = data["loginSession"]["rfSessionId"]
        self.api_url = data.get("loginArea", {}).get("apiDomain", self.api_url)
        self.user_code = data.get("loginUser", {}).get("userCode")
        self.session.headers.update({"sessionId": self.session_id})
        self.session_time = time.time()
        return data

    # ── повторное использование сессии────────────────────────────────────
    def load_token(self, path=None) -> bool:
        """Восстанавливает сохранённую сессию из файла (НЕ делает login).

        Возвращает: удалось ли восстановить. Если терминальный код другой
        (token скопирован с другого компьютера) — не восстанавливаем, облако всё равно отклонит."""
        path = path or self._default_token_file()
        try:
            with open(path, encoding="utf-8") as f:
                tok = json.load(f)
        except (OSError, ValueError):
            return False
        if not tok.get("session_id"):
            return False
        saved_fc = tok.get("feature_code")
        if saved_fc and saved_fc != self.FEATURE_CODE:
            return False
        self.session_id = tok["session_id"]
        self.rf_session_id = tok.get("rf_session_id")
        self.api_url = tok.get("api_url") or self.api_url
        self.email = tok.get("username") or self.email
        if tok.get("platform") in self.PLATFORMS:
            self.platform = tok["platform"]
            self.session.headers.update(self._base_headers())
        self.session_time = float(tok.get("session_time") or 0.0)
        self.session.headers.update({"sessionId": self.session_id})
        return True

    def is_refresh_needed(self, max_age=None) -> bool:
        """Нужно ли обновить сессию (по возрасту).

        Та же идея, что `is_refresh_login_needed()` в `hikconnect`:
        таймер «каждый час» не работает после пере-подключения/сна,
        а возраст всегда корректен."""
        if not self.session_id:
            return True
        age = time.time() - (self.session_time or 0.0)
        return age >= (self.SESSION_MAX_AGE if max_age is None else max_age)

    def connect(self, path=None):
        """Приводит сессию в ГОТОВЫЙ режим — по возможности БЕЗ login.

        Порядок: сохранённый token -> (если протух) refresh -> полный login.
        Прямой вызов `login()` при каждом запуске в облаке вызывает
        CAPTCHA (1015) и 2FA «новое устройство» — `ezviz-ha-addon`
        поэтому хранит сессию на диске именно для этого."""
        path = path or self._default_token_file()
        if self.load_token(path):
            if not self.is_refresh_needed():
                return "resumed"
            try:
                self.refresh_session(allow_login=False)
                self.save_token(path)
                return "refreshed"
            except Exception:
                pass                 # refresh не сработал -> полный login
        self.login()
        self.save_token(path)
        return "login"

    @staticmethod
    def _default_token_file() -> str:
        from .settings import get_active
        return get_active().token_file or "token.json"

    def refresh_session(self, allow_login=True):
        """
        Обновление сессии через refresh token — без запроса пароля/MFA.
        Критично для работы 24/7. При неудаче выполняет полный повторный login
        (`allow_login=False` — бросает исключение).
        """
        try:
            r = self.session.put(
                f"https://{self.api_url}/v3/apigateway/login",
                data={
                    "refreshSessionId": self.rf_session_id,
                    "featureCode": self.FEATURE_CODE,
                },
                timeout=25,
            )
            data = r.json()
            if data.get("meta", {}).get("code") == 200:
                info = data.get("sessionInfo", {})
                self.session_id = info.get("sessionId", self.session_id)
                self.rf_session_id = info.get("refreshSessionId", self.rf_session_id)
                self.session.headers.update({"sessionId": self.session_id})
                self.session_time = time.time()
                return True
        except Exception:
            pass
        # Если refresh не сработал — полный повторный login
        if not allow_login:
            raise RuntimeError("Не удалось обновить сессию (refresh token устарел)")
        self.login()
        return True

    def get_service_urls(self):
        r = self.session.get(
            f"https://{self.api_url}/v3/configurations/system/info", timeout=25)
        return r.json().get("systemConfigInfo", {})

    def get_devices(self):
        """Все камеры: {serial: {name, status, model, channels}}

        Получает постранично — облако отдаёт максимум 50 устройств за запрос,
        раньше свыше 50 просто терялись молча."""
        result = {}
        offset = 0
        for _ in range(40):          # 40 x 50 = 2000 устройств — защита от бесконечного цикла
            r = self.session.get(
                f"https://{self.api_url}/v3/userdevices/v1/devices/pagelist",
                params={
                    "filter": "CLOUD,CONNECTION,SWITCH,STATUS,WIFI,NODISTURB,"
                              "P2P,CHANNEL,VTM,FEATURE,UPGRADE,VIDEO_QUALITY,QOS",
                    "groupId": -1, "limit": 50, "offset": offset,
                },
                timeout=25,
            )
            data = r.json()
            devs = data.get("deviceInfos") or []
            for dev in devs:
                serial = dev.get("deviceSerial")
                if serial:
                    result[serial] = {
                        "name": dev.get("name", serial),
                        "status": dev.get("status", 0),
                        "model": dev.get("deviceType", ""),
                        "channels": dev.get("channelNumber", 1),
                    }
            page = data.get("page") or {}
            if not devs or not page.get("hasNext"):
                break
            offset += len(devs)
        return result

    def send_mfa_code(self):
        """
        Отправка кода 2FA (elevation) на email — нужна, если get_cam_key вернул 20002.
        Тот же endpoint, что в библиотеке pyezvizapi.
        """
        r = self.session.post(
            f"https://{self.api_url}/v3/sms/nologin/checkcode",
            data={"from": self.email, "bizType": "TERMINAL_BIND"},
            timeout=25,
        )
        return r.json()

    def get_cam_key(self, serial, channel=1, mfa_code=None):
        """
        Получение шифровального ключа камеры (encryptkey).

        EZVIZ защищает это через 2FA "elevation":
          - resultCode 0     -> ключ возвращается
          - resultCode 20002 -> нужен код 2FA (бросается MfaRequired)
          - resultCode 2009  -> устройство не подключено

        Важно: успешный вызов с mfa_code «повышает» (elevate) СЕССИЮ —
        после этого сохранённый через save_token() token позволяет прокси --decrypt-video
        получать ключ самостоятельно (без кода).
        """
        payload = {
            "checkcode": mfa_code,
            "serial": serial,
            "clientNo": "web_site",
            "clientType": 3,
            "netType": "WIFI",
            "featureCode": self.FEATURE_CODE,
            "sessionId": self.session_id,
        }
        r = self.session.post(
            f"https://{self.api_url}/api/device/query/encryptkey",
            data=payload, timeout=25,
        )
        data = r.json()
        code = str(data.get("resultCode"))
        if code == "0":
            return data.get("encryptkey")
        if code == "20002":
            raise MfaRequired("Требуется код 2FA (resultCode 20002)")
        if code == "10001":
            raise RuntimeError("Повторный запрос — подождите 1-2 минуты и повторите (10001)")
        if code == "2009":
            raise RuntimeError("Устройство не подключено (2009)")
        # Китайский resultDes не показываем, чтобы не было crash в консоли
        raise RuntimeError(f"Не удалось получить ключ (resultCode {code})")

    # ── код подтверждения (verification code)─────────────────────────────
    def send_verification_2fa(self, biz_type="DEVICE_AUTH_CODE"):
        """Отправляет код 2FA на email для получения кода подтверждения.

        В ответе `contact.fuzzyContact` — на какой адрес ушёл код."""
        r = self.session.post(
            f"https://{self.api_url}/v3/users/checkcode/mt",
            data={"bizType": biz_type, "from": self.email},
            timeout=25,
        )
        return r.json()

    def get_verification_code(self, serial, mfa_code=None):
        """Забирает из облака КОД ПОДТВЕРЖДЕНИЯ камеры (код с ярлыка).

        Именно этот код — AES-ключ для открытия потока. Раньше его приходилось
        вводить вручную через `set_code.py`; облако выдаёт его само
        (`pyezvizapi.get_cam_auth_code` использует тот же endpoint).

        При первом вызове облако запрашивает 2FA (meta.code 80000) ->
        `MfaRequired`. Тогда отправьте код через `send_verification_2fa()`
        и передайте код из email в `mfa_code`. Успешный 2FA
        ПОВЫШАЕТ СЕССИЮ — для остальных камер код не нужен.
        """
        params = {
            "encrptPwd": None,
            "msgAuthCode": mfa_code,
            "senderType": 3 if mfa_code else 0,
        }
        r = self.session.get(
            f"https://{self.api_url}/v3/devconfig/authcode/query/{serial}",
            params={k: v for k, v in params.items() if v is not None},
            timeout=25,
        )
        data = r.json()
        code = data.get("meta", {}).get("code")
        if code == 200 and data.get("devAuthCode"):
            return str(data["devAuthCode"]).strip().upper()
        if code == 80000:
            raise MfaRequired("Требуется код 2FA (meta.code 80000)")
        if code == 2009:
            raise RuntimeError("Устройство не подключено (2009)")
        raise RuntimeError(f"Не удалось получить код подтверждения (meta.code {code})")

    def save_token(self, path="ezviz_token.json"):
        """Сохраняет сессию на диск — при следующем запуске login НЕ будет.

        Также пишется `feature_code`: если token перенесён на другой компьютер,
        [[load_token]] отклонит его (облако всё равно не примет)."""
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "session_id": self.session_id,
                "rf_session_id": self.rf_session_id,
                "username": self.email,
                "api_url": self.api_url,
                "platform": self.platform,
                "feature_code": self.FEATURE_CODE,
                # Записывается реальный возраст: если неизвестно (0) — `connect()`
                # обновит. Если поставить `time.time()`, сессия будет «свежей»
                # и использовалась бы в протухшем состоянии.
                "session_time": self.session_time,
            }, f)
