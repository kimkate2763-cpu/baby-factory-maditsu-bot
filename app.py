import os
import hmac
import hashlib
import base64
import json
import logging
import re
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter

import psycopg
from psycopg.rows import dict_row

from flask import Flask, request, jsonify, abort


# =========================================================
# 기본 설정
# =========================================================

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger("baby-factory-maditsu-bot")

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 환경변수
# =========================================================

LINE_CHANNEL_SECRET = os.getenv(
    "LINE_CHANNEL_SECRET",
    "",
).strip()

LINE_CHANNEL_ACCESS_TOKEN = os.getenv(
    "LINE_CHANNEL_ACCESS_TOKEN",
    "",
).strip()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "",
).strip()

GEMINI_API_KEY = os.getenv(
    "GEMINI_API_KEY",
    "",
).strip()

GEMINI_MODEL = os.getenv(
    "GEMINI_MODEL",
    "gemini-3.8-flash",
).strip()

GEMINI_MODELS_ENV = os.getenv(
    "GEMINI_MODELS",
    "",
).strip()


# =========================================================
# Gemini 모델 설정
# =========================================================

DEFAULT_GEMINI_MODELS = [
    "gemini-3.8-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash-lite",
]

GEMINI_FAILURE_COOLDOWN = 60

GEMINI_PREFERRED_RETRY_COOLDOWN = 180

GEMINI_CONNECT_TIMEOUT = 1.2

GEMINI_READ_TIMEOUT = 5.0


_gemini_lock = threading.RLock()

_gemini_preferred_model = None

_gemini_failed_until = {}

_gemini_last_success = {}

_gemini_last_preferred_probe = 0.0


def get_configured_gemini_models():
    """
    Gemini 모델 목록을 만든다.

    우선순위:
    1. GEMINI_MODEL
    2. GEMINI_MODELS 환경변수
    3. 기본 fallback 목록
    """

    models = []

    if GEMINI_MODEL:
        models.append(GEMINI_MODEL)

    if GEMINI_MODELS_ENV:

        for model in GEMINI_MODELS_ENV.split(","):

            model = model.strip()

            if model and model not in models:
                models.append(model)

    for model in DEFAULT_GEMINI_MODELS:

        if model not in models:
            models.append(model)

    return models


# =========================================================
# HTTP Session
# =========================================================

http = requests.Session()

http.mount(
    "https://",
    HTTPAdapter(
        pool_connections=8,
        pool_maxsize=16,
        max_retries=0,
    ),
)


# =========================================================
# 날씨 API 설정
# Open-Meteo 사용
# 별도 API KEY 필요 없음
# =========================================================

WEATHER_GEOCODING_URL = (
    "https://geocoding-api.open-meteo.com/v1/search"
)

WEATHER_FORECAST_URL = (
    "https://api.open-meteo.com/v1/forecast"
)

WEATHER_CONNECT_TIMEOUT = 2.0

WEATHER_READ_TIMEOUT = 5.0


WEATHER_CODES = {
    0: "맑음 ☀️",
    1: "대체로 맑음 🌤️",
    2: "부분적으로 흐림 ⛅",
    3: "흐림 ☁️",
    45: "안개 🌫️",
    48: "안개 🌫️",
    51: "약한 이슬비 🌦️",
    53: "이슬비 🌦️",
    55: "강한 이슬비 🌧️",
    56: "어는 이슬비 🌧️",
    57: "강한 어는 이슬비 🌧️",
    61: "약한 비 🌧️",
    63: "비 🌧️",
    65: "강한 비 🌧️",
    66: "어는 비 🌧️",
    67: "강한 어는 비 🌧️",
    71: "약한 눈 🌨️",
    73: "눈 ❄️",
    75: "강한 눈 ❄️",
    77: "싸락눈 🌨️",
    80: "약한 소나기 🌦️",
    81: "소나기 🌧️",
    82: "강한 소나기 🌧️",
    85: "약한 눈소나기 🌨️",
    86: "강한 눈소나기 ❄️",
    95: "천둥번개 ⛈️",
    96: "천둥번개 + 우박 ⛈️",
    97: "강한 천둥번개 ⛈️",
    99: "강한 천둥번개 + 우박 ⛈️",
}


# =========================================================
# 날씨 지역명 추출
# =========================================================

def extract_weather_location(question):
    """
    예:
    오늘 혜화 근처 날씨 알려줘
    -> 혜화

    내일 서울 날씨
    -> 서울

    오늘 날씨 어때?
    -> None
    """

    text = question.strip()

    # 날짜 표현 제거
    text = re.sub(
        r"(오늘|내일|모레|지금|현재)",
        " ",
        text,
        flags=re.IGNORECASE,
    )

    # 날씨 관련 표현 제거
    text = re.sub(
        r"(날씨|기온|온도|강수량|강수|비|눈|"
        r"알려줘|알려 줘|어때|어때\?|궁금해|"
        r"몇 도|몇도)",
        " ",
        text,
        flags=re.IGNORECASE,
    )

    # 위치 관련 표현 제거
    text = re.sub(
        r"(근처|주변|쪽|지역|에서|의|에|은|는|이|가)",
        " ",
        text,
        flags=re.IGNORECASE,
    )

    # 문장부호 제거
    text = re.sub(
        r"[?!.,~]+",
        " ",
        text,
    )

    # 공백 정리
    text = re.sub(
        r"\s+",
        " ",
        text,
    ).strip()

    if not text:
        return None

    if len(text) > 40:
        return None

    return text


# =========================================================
# 날씨 지역 검색
# =========================================================

def get_weather_location(
    location_text,
):

    if not location_text:
        return None

    response = http.get(
        WEATHER_GEOCODING_URL,
        params={
            "name": location_text,
            "count": 5,
            "language": "ko",
            "format": "json",
            "countryCode": "KR",
        },
        timeout=(
            WEATHER_CONNECT_TIMEOUT,
            WEATHER_READ_TIMEOUT,
        ),
    )

    if response.status_code != 200:

        raise RuntimeError(
            "날씨 지역 검색 실패: "
            f"HTTP {response.status_code}"
        )

    data = response.json()

    results = data.get(
        "results"
    ) or []

    if not results:
        return None

    # 한국 결과 우선
    selected = None

    for result in results:

        country_code = str(
            result.get(
                "country_code",
                ""
            )
        ).upper()

        if country_code == "KR":

            selected = result
            break

    if selected is None:
        selected = results[0]

    return {
        "name": selected.get(
            "name"
        ) or location_text,
        "admin1": selected.get(
            "admin1"
        ) or "",
        "latitude": selected.get(
            "latitude"
        ),
        "longitude": selected.get(
            "longitude"
        ),
        "timezone": selected.get(
            "timezone"
        ) or "Asia/Seoul",
    }


# =========================================================
# 날씨 데이터 조회
# =========================================================

def get_weather_data(
    location,
):

    response = http.get(
        WEATHER_FORECAST_URL,
        params={
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "current": (
                "temperature_2m,"
                "apparent_temperature,"
                "relative_humidity_2m,"
                "weather_code,"
                "wind_speed_10m"
            ),
            "daily": (
                "weather_code,"
                "temperature_2m_max,"
                "temperature_2m_min,"
                "apparent_temperature_max,"
                "apparent_temperature_min,"
                "precipitation_probability_max,"
                "precipitation_sum,"
                "sunrise,"
                "sunset"
            ),
            "timezone": location.get(
                "timezone",
                "Asia/Seoul",
            ),
            "forecast_days": 3,
        },
        timeout=(
            WEATHER_CONNECT_TIMEOUT,
            WEATHER_READ_TIMEOUT,
        ),
    )

    if response.status_code != 200:

        raise RuntimeError(
            "날씨 조회 실패: "
            f"HTTP {response.status_code}"
        )

    data = response.json()

    if data.get("error"):

        raise RuntimeError(
            data.get(
                "reason",
                "날씨 API 오류",
            )
        )

    return data


# =========================================================
# 날씨 날짜 판단
# =========================================================

def get_weather_target_day(
    question,
):

    text = question.lower()

    if "모레" in text:
        return 2

    if "내일" in text:
        return 1

    return 0


# =========================================================
# 날씨 결과 포맷
# =========================================================

def format_weather_answer(
    location,
    weather,
    target_day,
):

    daily = weather.get(
        "daily"
    ) or {}

    dates = daily.get(
        "time"
    ) or []

    codes = daily.get(
        "weather_code"
    ) or []

    max_temps = daily.get(
        "temperature_2m_max"
    ) or []

    min_temps = daily.get(
        "temperature_2m_min"
    ) or []

    apparent_max = daily.get(
        "apparent_temperature_max"
    ) or []

    apparent_min = daily.get(
        "apparent_temperature_min"
    ) or []

    rain_probs = daily.get(
        "precipitation_probability_max"
    ) or []

    precipitation = daily.get(
        "precipitation_sum"
    ) or []

    sunrise = daily.get(
        "sunrise"
    ) or []

    sunset = daily.get(
        "sunset"
    ) or []

    if not dates:
        raise RuntimeError(
            "날씨 일별 데이터가 없습니다."
        )

    if target_day >= len(dates):

        target_day = 0

    date_text = dates[target_day]

    code = (
        codes[target_day]
        if target_day < len(codes)
        else None
    )

    try:

        weather_code = int(code)

    except (
        TypeError,
        ValueError,
    ):

        weather_code = -1

    condition = WEATHER_CODES.get(
        weather_code,
        "날씨 정보 확인 필요 🌥️",
    )

    max_temp = (
        max_temps[target_day]
        if target_day < len(max_temps)
        else None
    )

    min_temp = (
        min_temps[target_day]
        if target_day < len(min_temps)
        else None
    )

    apparent_high = (
        apparent_max[target_day]
        if target_day < len(apparent_max)
        else None
    )

    apparent_low = (
        apparent_min[target_day]
        if target_day < len(apparent_min)
        else None
    )

    rain_probability = (
        rain_probs[target_day]
        if target_day < len(rain_probs)
        else None
    )

    precipitation_amount = (
        precipitation[target_day]
        if target_day < len(precipitation)
        else None
    )

    day_names = {
        0: "오늘",
        1: "내일",
        2: "모레",
    }

    day_label = day_names.get(
        target_day,
        "오늘",
    )

    lines = [
        f"🧸 {location['name']} {day_label} 날씨",
        f"📅 {date_text}",
        f"🌤️ {condition}",
    ]

    if (
        min_temp is not None
        and max_temp is not None
    ):

        lines.append(
            f"🌡️ 기온 "
            f"{float(min_temp):.0f}°C ~ "
            f"{float(max_temp):.0f}°C"
        )

    if (
        apparent_low is not None
        and apparent_high is not None
    ):

        lines.append(
            f"🥶 체감 "
            f"{float(apparent_low):.0f}°C ~ "
            f"{float(apparent_high):.0f}°C"
        )

    if rain_probability is not None:

        lines.append(
            f"☔ 강수확률 최대 "
            f"{float(rain_probability):.0f}%"
        )

    if precipitation_amount is not None:

        lines.append(
            f"💧 예상 강수량 "
            f"{float(precipitation_amount):.1f}mm"
        )

    # 오늘인 경우 현재 날씨 추가
    if (
        target_day == 0
        and weather.get("current")
    ):

        current = weather[
            "current"
        ]

        current_temp = current.get(
            "temperature_2m"
        )

        current_apparent = current.get(
            "apparent_temperature"
        )

        current_humidity = current.get(
            "relative_humidity_2m"
        )

        current_wind = current.get(
            "wind_speed_10m"
        )

        if current_temp is not None:

            lines.append(
                f"🌡️ 현재 "
                f"{float(current_temp):.1f}°C"
            )

        if current_apparent is not None:

            lines.append(
                f"🧣 현재 체감 "
                f"{float(current_apparent):.1f}°C"
            )

        if current_humidity is not None:

            lines.append(
                f"💦 습도 "
                f"{float(current_humidity):.0f}%"
            )

        if current_wind is not None:

            lines.append(
                f"💨 바람 "
                f"{float(current_wind):.0f}km/h"
            )

    if (
        target_day < len(sunrise)
        and target_day < len(sunset)
    ):

        sunrise_text = str(
            sunrise[target_day]
        )[-5:]

        sunset_text = str(
            sunset[target_day]
        )[-5:]

        lines.append(
            f"🌅 일출 {sunrise_text} · "
            f"일몰 {sunset_text}"
        )

    lines.append("")

    lines.append(
        "🧸 장난감 가게 기상대에서 확인했어!"
    )

    return "\n".join(
        lines
    )[:4900]


# =========================================================
# 날씨 질문 처리
# =========================================================

def ask_weather(
    question,
):

    location_text = extract_weather_location(
        question
    )

    if not location_text:

        return (
            "어디 날씨를 볼까? 🧸☁️\n"
            "지역을 같이 적어줘!\n"
            "예: !오늘 혜화 근처 날씨 알려줘"
        )

    try:

        location = get_weather_location(
            location_text
        )

        if not location:

            return (
                f"앗, '{location_text}' 지역을 "
                "찾지 못했어 🧸💦\n"
                "조금 더 정확한 지역명을 적어줘!"
            )

        weather = get_weather_data(
            location
        )

        target_day = get_weather_target_day(
            question
        )

        return format_weather_answer(
            location,
            weather,
            target_day,
        )

    except Exception as exc:

        logger.warning(
            "날씨 조회 실패: "
            "location=%r error=%s",
            location_text,
            exc,
            exc_info=True,
        )

        return (
            "앗... 장난감 가게 기상대가 "
            "잠깐 삐끗했어 🧸🌧️\n"
            "조금 있다가 다시 물어봐줘!"
        )


# =========================================================
# DB 설정
# =========================================================

_db_lock = threading.RLock()

_db_connection = None

_db_connection_created_at = 0.0

_db_init_lock = threading.Lock()

_db_initialized = False

DB_CONNECT_TIMEOUT = 5

DB_RECONNECT_AFTER = 1800

DB_STATEMENT_TIMEOUT = 8000


# =========================================================
# DB 연결 종료
# =========================================================

def _close_db_locked():

    global _db_connection
    global _db_connection_created_at

    conn = _db_connection

    _db_connection = None
    _db_connection_created_at = 0.0

    if conn is not None:

        try:
            conn.close()

        except Exception:
            pass


# =========================================================
# DB 연결 생성
# =========================================================

def _connect_db_locked():

    global _db_connection
    global _db_connection_created_at

    if not DATABASE_URL:

        raise RuntimeError(
            "DATABASE_URL 환경변수가 설정되지 않았습니다."
        )

    _close_db_locked()

    conn = psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=DB_CONNECT_TIMEOUT,
        autocommit=False,
    )

    conn.execute(
        f"SET statement_timeout = "
        f"'{DB_STATEMENT_TIMEOUT}ms'"
    )

    conn.commit()

    _db_connection = conn

    _db_connection_created_at = (
        time.monotonic()
    )

    logger.info(
        "DB 연결 생성 완료"
    )

    return conn


# =========================================================
# DB 연결 가져오기
# =========================================================

def _get_connection_locked():

    global _db_connection

    if _db_connection is None:

        return _connect_db_locked()

    try:

        if _db_connection.closed:

            return _connect_db_locked()

    except Exception:

        return _connect_db_locked()

    if (
        time.monotonic()
        - _db_connection_created_at
        >= DB_RECONNECT_AFTER
    ):

        logger.info(
            "DB 연결 수명 갱신"
        )

        return _connect_db_locked()

    return _db_connection


# =========================================================
# DB 연결 오류 판별
# =========================================================

def _is_connection_error(
    exc,
):

    if isinstance(
        exc,
        (
            psycopg.OperationalError,
            psycopg.InterfaceError,
        ),
    ):

        return True

    text = str(exc).lower()

    connection_keywords = (
        "server closed",
        "connection is closed",
        "connection not open",
        "connection refused",
        "connection reset",
        "ssl connection",
        "broken pipe",
        "terminating connection",
        "connection timed out",
        "could not connect",
    )

    return any(
        keyword in text
        for keyword in connection_keywords
    )


# =========================================================
# DB 테이블 초기화
# =========================================================

def init_database():

    global _db_initialized

    if _db_initialized:
        return

    with _db_init_lock:

        if _db_initialized:
            return

        with _db_lock:

            conn = _get_connection_locked()

            try:

                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS message_events (
                        event_id TEXT PRIMARY KEY,
                        group_id TEXT NOT NULL,
                        user_id TEXT NOT NULL,
                        display_name TEXT NOT NULL
                            DEFAULT '이름 미확인',
                        message_date DATE NOT NULL,
                        character_count INTEGER NOT NULL
                            DEFAULT 0,
                        created_at TIMESTAMPTZ NOT NULL
                            DEFAULT NOW()
                    )
                    """
                )

                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_message_group_date
                    ON message_events
                    (group_id, message_date)
                    """
                )

                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_message_group_user_date
                    ON message_events
                    (group_id, user_id, message_date)
                    """
                )

                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_message_group_date_user
                    ON message_events
                    (group_id, message_date, user_id)
                    """
                )

                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS
                    group_authenticators (
                        group_id TEXT NOT NULL,
                        user_id TEXT NOT NULL,
                        display_name TEXT NOT NULL
                            DEFAULT '이름 미확인',
                        registered_at TIMESTAMPTZ
                            NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (
                            group_id,
                            user_id
                        )
                    )
                    """
                )

                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS
                    group_admins (
                        group_id TEXT NOT NULL,
                        user_id TEXT NOT NULL,
                        display_name TEXT NOT NULL
                            DEFAULT '이름 미확인',
                        registered_at TIMESTAMPTZ
                            NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (
                            group_id,
                            user_id
                        )
                    )
                    """
                )

                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS
                    bot_runtime_state (
                        state_key TEXT PRIMARY KEY,
                        state_value TEXT,
                        updated_at TIMESTAMPTZ
                            NOT NULL DEFAULT NOW()
                    )
                    """
                )

                conn.commit()

                _db_initialized = True

                logger.info(
                    "데이터베이스 초기화 완료"
                )

            except Exception:

                try:
                    conn.rollback()

                except Exception:
                    pass

                raise


# =========================================================
# DB 실행
# =========================================================

def db_execute(
    sql,
    params=None,
    *,
    fetchone=False,
    fetchall=False,
):

    global _db_connection

    if not _db_initialized:

        init_database()

    last_error = None

    for attempt in range(2):

        try:

            with _db_lock:

                conn = _get_connection_locked()

                try:

                    cur = conn.execute(
                        sql,
                        params,
                    )

                    if fetchone:

                        result = cur.fetchone()

                    elif fetchall:

                        result = cur.fetchall()

                    else:

                        result = cur.rowcount

                    conn.commit()

                    return result

                except Exception as exc:

                    try:
                        conn.rollback()

                    except Exception:
                        pass

                    if _is_connection_error(
                        exc
                    ):

                        logger.warning(
                            "DB 연결 오류. "
                            "연결 재생성: "
                            "attempt=%s error=%s",
                            attempt + 1,
                            exc,
                        )

                        _close_db_locked()

                        last_error = exc

                    else:

                        raise

        except Exception as exc:

            last_error = exc

            if (
                attempt == 0
                and _is_connection_error(
                    exc
                )
            ):

                time.sleep(0.2)

                continue

            raise

        if last_error is not None:

            time.sleep(0.2)

    if last_error is not None:

        raise last_error

    raise RuntimeError(
        "DB 작업 실패"
    )


def db_fetchone(
    sql,
    params=None,
):

    return db_execute(
        sql,
        params,
        fetchone=True,
    )


def db_fetchall(
    sql,
    params=None,
):

    return (
        db_execute(
            sql,
            params,
            fetchall=True,
        )
        or []
    )


# =========================================================
# LINE 공통 헤더
# =========================================================

def line_headers():

    return {
        "Authorization":
            f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type":
            "application/json",
    }


# =========================================================
# LINE 프로필 캐시
# =========================================================

_PROFILE_CACHE = {}

_PROFILE_CACHE_LOCK = threading.Lock()

_PROFILE_CACHE_TTL = 600

_PROFILE_LOOKUPS_IN_FLIGHT = set()

_PROFILE_EXECUTOR = ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="line-profile",
)


def _profile_url(
    source,
    group_id,
    user_id,
):

    source_type = source.get(
        "type"
    )

    if (
        source_type == "group"
        and group_id
    ):

        return (
            "https://api.line.me/v2/bot/group/"
            f"{group_id}/member/{user_id}"
        )

    if (
        source_type == "room"
        and group_id
    ):

        return (
            "https://api.line.me/v2/bot/room/"
            f"{group_id}/member/{user_id}"
        )

    return (
        "https://api.line.me/v2/bot/profile/"
        f"{user_id}"
    )


# =========================================================
# DB에 실제 이름 저장
# =========================================================

def _save_profile_name(
    group_id,
    user_id,
    name,
):

    db_execute(
        """
        UPDATE message_events
        SET display_name = %s
        WHERE group_id = %s
          AND user_id = %s
          AND (
              display_name LIKE '사용자-%%'
              OR display_name = '이름 미확인'
          )
        """,
        (
            name,
            group_id,
            user_id,
        ),
    )

    db_execute(
        """
        UPDATE group_authenticators
        SET display_name = %s
        WHERE group_id = %s
          AND user_id = %s
          AND (
              display_name LIKE '사용자-%%'
              OR display_name = '이름 미확인'
          )
        """,
        (
            name,
            group_id,
            user_id,
        ),
    )

    db_execute(
        """
        UPDATE group_admins
        SET display_name = %s
        WHERE group_id = %s
          AND user_id = %s
          AND (
              display_name LIKE '사용자-%%'
              OR display_name = '이름 미확인'
          )
        """,
        (
            name,
            group_id,
            user_id,
        ),
    )


# =========================================================
# 프로필 조회
# =========================================================

def _fetch_profile_name(
    source,
    group_id,
    user_id,
    cache_key,
):

    try:

        if not LINE_CHANNEL_ACCESS_TOKEN:
            return None

        response = http.get(
            _profile_url(
                source,
                group_id,
                user_id,
            ),
            headers=line_headers(),
            timeout=(1.5, 2.5),
        )

        if response.status_code == 404:

            logger.info(
                "프로필을 찾을 수 없음: %s",
                user_id,
            )

            return None

        if not response.ok:

            logger.warning(
                "프로필 조회 실패: "
                "status=%s body=%s",
                response.status_code,
                response.text[:300],
            )

            return None

        data = response.json()

        name = data.get(
            "displayName"
        )

        if not name:
            return None

        with _PROFILE_CACHE_LOCK:

            _PROFILE_CACHE[
                cache_key
            ] = (
                time.time(),
                name,
            )

        try:

            _save_profile_name(
                group_id,
                user_id,
                name,
            )

        except Exception:

            logger.exception(
                "프로필 이름 DB 갱신 실패"
            )

        return name

    except requests.RequestException:

        logger.warning(
            "프로필 조회 네트워크 오류",
            exc_info=True,
        )

        return None

    except Exception:

        logger.exception(
            "프로필 처리 중 오류"
        )

        return None

    finally:

        with _PROFILE_CACHE_LOCK:

            _PROFILE_LOOKUPS_IN_FLIGHT.discard(
                cache_key
            )


# =========================================================
# 최근 DB 이름 조회
# =========================================================

def get_recent_display_name(
    group_id,
    user_id,
):

    try:

        row = db_fetchone(
            """
            SELECT display_name
            FROM message_events
            WHERE group_id = %s
              AND user_id = %s
              AND display_name IS NOT NULL
              AND display_name <> ''
              AND display_name NOT LIKE '사용자-%%'
              AND display_name <> '이름 미확인'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                group_id,
                user_id,
            ),
        )

        if row:

            return row.get(
                "display_name"
            )

    except Exception:

        logger.warning(
            "최근 표시 이름 조회 실패",
            exc_info=True,
        )

    return None


# =========================================================
# 표시 이름 가져오기
# =========================================================

def get_display_name(
    source,
    group_id,
    user_id,
    *,
    force_fetch=False,
):

    fallback_name = (
        "사용자-" + user_id[-6:]
    )

    cache_key = (
        source.get("type"),
        group_id,
        user_id,
    )

    with _PROFILE_CACHE_LOCK:

        cached = _PROFILE_CACHE.get(
            cache_key
        )

        if (
            cached
            and time.time() - cached[0]
            < _PROFILE_CACHE_TTL
        ):

            return cached[1]

    recent_name = get_recent_display_name(
        group_id,
        user_id,
    )

    if recent_name:

        with _PROFILE_CACHE_LOCK:

            _PROFILE_CACHE[
                cache_key
            ] = (
                time.time(),
                recent_name,
            )

        if not force_fetch:

            return recent_name

    if force_fetch:

        name = _fetch_profile_name(
            source,
            group_id,
            user_id,
            cache_key,
        )

        if name:

            return name

    return (
        recent_name
        or fallback_name
    )


# =========================================================
# 프로필 비동기 조회
# =========================================================

def schedule_profile_lookup(
    source,
    group_id,
    user_id,
):

    if not LINE_CHANNEL_ACCESS_TOKEN:
        return

    cache_key = (
        source.get("type"),
        group_id,
        user_id,
    )

    with _PROFILE_CACHE_LOCK:

        cached = _PROFILE_CACHE.get(
            cache_key
        )

        if (
            cached
            and time.time() - cached[0]
            < _PROFILE_CACHE_TTL
        ):

            return

        if cache_key in _PROFILE_LOOKUPS_IN_FLIGHT:

            return

        _PROFILE_LOOKUPS_IN_FLIGHT.add(
            cache_key
        )

    try:

        _PROFILE_EXECUTOR.submit(
            _fetch_profile_name,
            source,
            group_id,
            user_id,
            cache_key,
        )

    except Exception:

        with _PROFILE_CACHE_LOCK:

            _PROFILE_LOOKUPS_IN_FLIGHT.discard(
                cache_key
            )

        logger.exception(
            "프로필 조회 작업 예약 실패"
        )


# =========================================================
# LINE 일반 답장
# =========================================================

def reply_text(
    reply_token,
    text,
):

    if (
        not reply_token
        or not LINE_CHANNEL_ACCESS_TOKEN
    ):

        return False

    try:

        response = http.post(
            "https://api.line.me/v2/bot/message/reply",
            headers=line_headers(),
            json={
                "replyToken": reply_token,
                "messages": [
                    {
                        "type": "text",
                        "text": str(text)[:4900],
                    }
                ],
            },
            timeout=(2.5, 5),
        )

        if not response.ok:

            logger.error(
                "LINE 답장 실패: "
                "status=%s body=%s",
                response.status_code,
                response.text[:1000],
            )

            return False

        return True

    except requests.RequestException:

        logger.exception(
            "LINE 답장 전송 오류"
        )

        return False


# =========================================================
# 그룹 멤버 존재 확인
# =========================================================

def _member_exists_in_group(
    source,
    group_id,
    user_id,
):

    if (
        not LINE_CHANNEL_ACCESS_TOKEN
        or not user_id
    ):

        return None

    try:

        response = http.get(
            _profile_url(
                source,
                group_id,
                user_id,
            ),
            headers=line_headers(),
            timeout=(1.5, 2.5),
        )

        if response.status_code == 200:

            return True

        if response.status_code == 404:

            logger.warning(
                "현재 그룹에 없는 멤버 감지: "
                "group_id=%s user_id=%s",
                group_id,
                user_id,
            )

            return False

        logger.warning(
            "그룹 멤버 확인 실패: "
            "status=%s group_id=%s user_id=%s",
            response.status_code,
            group_id,
            user_id,
        )

        return None

    except requests.RequestException:

        return None


# =========================================================
# 유효한 멘션 대상
# =========================================================

def get_valid_mention_members(
    source,
    group_id,
    members,
    *,
    table_name,
):

    valid_members = []

    stale_user_ids = []

    for member in members:

        user_id = member.get(
            "user_id"
        )

        if not user_id:
            continue

        status = _member_exists_in_group(
            source,
            group_id,
            user_id,
        )

        if status is True:

            valid_members.append(
                member
            )

        elif status is False:

            stale_user_ids.append(
                user_id
            )

    if stale_user_ids:

        if table_name == "admins":

            for user_id in stale_user_ids:

                db_execute(
                    """
                    DELETE FROM group_admins
                    WHERE group_id = %s
                      AND user_id = %s
                    """,
                    (
                        group_id,
                        user_id,
                    ),
                )

        elif table_name == "authenticators":

            for user_id in stale_user_ids:

                db_execute(
                    """
                    DELETE FROM group_authenticators
                    WHERE group_id = %s
                      AND user_id = %s
                    """,
                    (
                        group_id,
                        user_id,
                    ),
                )

        logger.info(
            "현재 방에 없는 %s 자동 정리: count=%s",
            table_name,
            len(stale_user_ids),
        )

    return valid_members


# =========================================================
# LINE 멘션 답장
# =========================================================

def reply_mention(
    reply_token,
    text,
    members,
):

    if (
        not reply_token
        or not LINE_CHANNEL_ACCESS_TOKEN
    ):

        return False

    if not members:

        return reply_text(
            reply_token,
            text,
        )

    valid_members = [
        member
        for member in members
        if (
            member.get("user_id")
            and str(
                member["user_id"]
            ).startswith("U")
        )
    ][:20]

    if not valid_members:

        return reply_text(
            reply_token,
            text
            + "\n등록된 사용자 정보를 확인해 주세요.",
        )

    substitution = {}

    placeholders = []

    for index, member in enumerate(
        valid_members,
        start=1,
    ):

        key = f"member{index}"

        placeholders.append(
            "{" + key + "}"
        )

        substitution[key] = {
            "type": "mention",
            "mentionee": {
                "type": "user",
                "userId": member["user_id"],
            },
        }

    base_text = str(
        text
    ).rstrip()

    message_text = (
        base_text
        + "\n"
        + " ".join(placeholders)
    )

    try:

        response = http.post(
            "https://api.line.me/v2/bot/message/reply",
            headers=line_headers(),
            json={
                "replyToken": reply_token,
                "messages": [
                    {
                        "type": "textV2",
                        "text": message_text[:4900],
                        "substitution": substitution,
                    }
                ],
            },
            timeout=(2.5, 5),
        )

        if not response.ok:

            logger.error(
                "LINE 멘션 실패: "
                "status=%s body=%s",
                response.status_code,
                response.text[:1200],
            )

            return False

        return True

    except requests.RequestException:

        logger.exception(
            "LINE 멘션 전송 오류"
        )

        return False


# =========================================================
# 메시지 기록
# =========================================================

def record_message(
    event_id,
    group_id,
    user_id,
    display_name,
    message_date,
    character_count,
):

    started = time.monotonic()

    db_execute(
        """
        INSERT INTO message_events
            (
                event_id,
                group_id,
                user_id,
                display_name,
                message_date,
                character_count
            )
        VALUES
            (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id)
        DO NOTHING
        """,
        (
            event_id,
            group_id,
            user_id,
            display_name,
            message_date,
            character_count,
        ),
    )

    elapsed = (
        time.monotonic()
        - started
    )

    if elapsed >= 1:

        logger.warning(
            "메시지 DB 기록 지연: %.2f초",
            elapsed,
        )


# =========================================================
# 인증자
# =========================================================

def register_authenticator(
    group_id,
    user_id,
    display_name,
):

    db_execute(
        """
        INSERT INTO group_authenticators
            (
                group_id,
                user_id,
                display_name
            )
        VALUES
            (%s, %s, %s)
        ON CONFLICT
            (group_id, user_id)
        DO UPDATE SET
            display_name = EXCLUDED.display_name,
            registered_at = NOW()
        """,
        (
            group_id,
            user_id,
            display_name,
        ),
    )


def unregister_authenticator(
    group_id,
    user_id,
):

    return db_execute(
        """
        DELETE FROM group_authenticators
        WHERE group_id = %s
          AND user_id = %s
        """,
        (
            group_id,
            user_id,
        ),
    )


def get_authenticators(
    group_id,
):

    return db_fetchall(
        """
        SELECT
            user_id,
            display_name
        FROM group_authenticators
        WHERE group_id = %s
        ORDER BY registered_at ASC
        """,
        (
            group_id,
        ),
    )


# =========================================================
# 관리자
# =========================================================

def register_admin(
    group_id,
    user_id,
    display_name,
):

    db_execute(
        """
        INSERT INTO group_admins
            (
                group_id,
                user_id,
                display_name
            )
        VALUES
            (%s, %s, %s)
        ON CONFLICT
            (group_id, user_id)
        DO UPDATE SET
            display_name = EXCLUDED.display_name,
            registered_at = NOW()
        """,
        (
            group_id,
            user_id,
            display_name,
        ),
    )


def unregister_admin(
    group_id,
    user_id,
):

    return db_execute(
        """
        DELETE FROM group_admins
        WHERE group_id = %s
          AND user_id = %s
        """,
        (
            group_id,
            user_id,
        ),
    )


def get_admins(
    group_id,
):

    return db_fetchall(
        """
        SELECT
            user_id,
            display_name
        FROM group_admins
        WHERE group_id = %s
        ORDER BY registered_at ASC
        """,
        (
            group_id,
        ),
    )


# =========================================================
# 주간 날짜
# =========================================================

def get_week_start(
    target_date,
):

    return (
        target_date
        - timedelta(
            days=target_date.weekday()
        )
    )


def get_week_end(
    target_date,
):

    return (
        get_week_start(target_date)
        + timedelta(days=6)
    )


# =========================================================
# 주간 칭호
# =========================================================

WEEKLY_TITLE_TABLE = [
    (100, "🕶️ 마딧수 견습생"),
    (500, "⚔️ 공창의 전사"),
    (1000, "🔥 마딧수 폭주자"),
    (2000, "👑 공창 지배자"),
    (3000, "☠️ 마딧수 사신"),
    (4000, "⚡ 채팅의 초월자"),
    (5000, "🌌 공창의 절대자"),
    (6000, "🩸 말의 파괴신"),
    (7000, "👁️ 언어의 지배자"),
    (8000, "💀 채팅창의 재앙"),
    (9000, "⚡ 공창의 초월신"),
    (10000, "👑 마딧수의 신좌"),
    (11000, "🌑 침묵을 멸한 자"),
    (12000, "🔥 언어의 절대군주"),
    (13000, "☄️ 채팅의 종언자"),
    (14000, "🪽 공창의 초월자"),
    (15000, "👑 말의 지배신"),
    (16000, "🌌 채팅의 창조신"),
    (17000, "☠️ 공창의 최종보스"),
    (18000, "⚡ 언어의 절대자"),
    (19000, "🌠 마딧수의 신"),
    (20000, "👑 채팅창의 유일신"),
]


def get_weekly_title(
    message_count,
):

    if message_count < 100:

        return "칭호 없음"

    current_title = "칭호 없음"

    for threshold, title in WEEKLY_TITLE_TABLE:

        if message_count >= threshold:

            current_title = title

        else:

            break

    if message_count >= 20000:

        extra_level = (
            message_count - 20000
        ) // 1000

        if extra_level > 0:

            current_title = (
                "👑 채팅창의 유일신 "
                f"— 초월 {extra_level + 1}단계"
            )

    return current_title


# =========================================================
# 주간 개인 통계
# =========================================================

def get_weekly_stats(
    group_id,
    user_id,
    today,
):

    week_start = get_week_start(
        today
    )

    week_end = get_week_end(
        today
    )

    row = db_fetchone(
        """
        WITH weekly_counts AS (
            SELECT
                user_id,
                COUNT(*) AS message_count
            FROM message_events
            WHERE group_id = %s
              AND message_date >= %s
              AND message_date <= %s
            GROUP BY user_id
        ),
        ranked AS (
            SELECT
                user_id,
                message_count,
                RANK() OVER (
                    ORDER BY message_count DESC
                ) AS weekly_rank
            FROM weekly_counts
        )
        SELECT
            message_count,
            weekly_rank
        FROM ranked
        WHERE user_id = %s
        """,
        (
            group_id,
            week_start,
            week_end,
            user_id,
        ),
    )

    if row:

        count = row[
            "message_count"
        ]

        rank = row[
            "weekly_rank"
        ]

    else:

        count = 0
        rank = "-"

    title = get_weekly_title(
        count
    )

    return (
        count,
        rank,
        title,
        week_start,
        week_end,
    )


# =========================================================
# 주간 전체 순위
# =========================================================

def get_weekly_ranking(
    group_id,
    today,
):

    week_start = get_week_start(
        today
    )

    week_end = get_week_end(
        today
    )

    rows = db_fetchall(
        """
        SELECT
            user_id,
            COALESCE(
                MAX(display_name)
                FILTER (
                    WHERE display_name
                    NOT LIKE '사용자-%%'
                    AND display_name <> '이름 미확인'
                ),
                MAX(display_name)
            ) AS display_name,
            COUNT(*) AS message_count
        FROM message_events
        WHERE group_id = %s
          AND message_date >= %s
          AND message_date <= %s
        GROUP BY user_id
        ORDER BY
            message_count DESC,
            LOWER(
                COALESCE(
                    MAX(display_name)
                    FILTER (
                        WHERE display_name
                        NOT LIKE '사용자-%%'
                        AND display_name <> '이름 미확인'
                    ),
                    MAX(display_name)
                )
            ) ASC
        LIMIT 100
        """,
        (
            group_id,
            week_start,
            week_end,
        ),
    )

    lines = [
        "👑 이번 주 마딧수 순위",
        (
            f"기간: {week_start} ~ {week_end}"
        ),
        "",
    ]

    if not rows:

        lines.extend([
            "아직 이번 주 기록이 없습니다.",
            "이번 주 월요일부터 다시 집계됩니다.",
        ])

        return "\n".join(
            lines
        )

    for rank, row in enumerate(
        rows,
        start=1,
    ):

        count = row[
            "message_count"
        ]

        title = get_weekly_title(
            count
        )

        if title == "칭호 없음":

            title_text = ""

        else:

            title_text = (
                f" 「{title}」"
            )

        lines.append(
            f"{rank}위. "
            f"{row['display_name']} - "
            f"{count:,}마디"
            f"{title_text}"
        )

    lines.extend([
        "",
        "※ 매주 월요일 00:00부터 새 주간으로 집계됩니다.",
        "※ 칭호는 가장 높은 단계 하나만 적용됩니다.",
        "※ 지난 기록은 삭제되지 않습니다.",
        "※ 순위는 최대 100명까지 표시됩니다.",
    ])

    return "\n".join(
        lines
    )


# =========================================================
# 내 마딧수
# =========================================================

def get_my_stats(
    group_id,
    user_id,
    today,
    display_name,
):

    row = db_fetchone(
        """
        WITH daily_counts AS (
            SELECT
                user_id,
                COUNT(*) AS message_count,
                COALESCE(
                    SUM(character_count),
                    0
                ) AS character_count
            FROM message_events
            WHERE group_id = %s
              AND message_date = %s
            GROUP BY user_id
        ),
        ranked AS (
            SELECT
                user_id,
                message_count,
                character_count,
                RANK() OVER (
                    ORDER BY message_count DESC
                ) AS user_rank
            FROM daily_counts
        )
        SELECT
            message_count,
            character_count,
            user_rank
        FROM ranked
        WHERE user_id = %s
        """,
        (
            group_id,
            today,
            user_id,
        ),
    )

    if row:

        count = row[
            "message_count"
        ]

        chars = row[
            "character_count"
        ]

        rank = row[
            "user_rank"
        ]

    else:

        count = 0
        chars = 0
        rank = "-"

    (
        weekly_count,
        weekly_rank,
        weekly_title,
        week_start,
        week_end,
    ) = get_weekly_stats(
        group_id,
        user_id,
        today,
    )

    if weekly_title == "칭호 없음":

        title_text = (
            "아직 칭호가 없습니다.\n"
            "이번 주 100마디를 달성하면 "
            "첫 칭호를 얻어요!"
        )

    else:

        title_text = (
            f"「{weekly_title}」"
        )

    return (
        "📊 오늘의 마딧수\n"
        f"이름: {display_name}\n"
        f"날짜: {today}\n"
        f"내 마딧수: {count:,}마디\n"
        f"공백 제외 글자 수: {chars:,}자\n"
        f"오늘의 순위: {rank}위\n"
        "\n"
        "👑 이번 주 마딧수\n"
        f"기간: {week_start} ~ {week_end}\n"
        f"이번 주 마딧수: {weekly_count:,}마디\n"
        f"이번 주 순위: {weekly_rank}위\n"
        f"이번 주 칭호: {title_text}"
    )


# =========================================================
# 일반 마딧수 순위
# =========================================================

def get_ranking(
    group_id,
    start_date,
    end_date,
    title,
):

    rows = db_fetchall(
        """
        SELECT
            user_id,
            COALESCE(
                MAX(display_name)
                FILTER (
                    WHERE display_name
                    NOT LIKE '사용자-%%'
                    AND display_name <> '이름 미확인'
                ),
                MAX(display_name)
            ) AS display_name,
            COUNT(*) AS message_count
        FROM message_events
        WHERE group_id = %s
          AND message_date >= %s
          AND message_date <= %s
        GROUP BY user_id
        ORDER BY
            message_count DESC,
            LOWER(
                COALESCE(
                    MAX(display_name)
                    FILTER (
                        WHERE display_name
                        NOT LIKE '사용자-%%'
                        AND display_name <> '이름 미확인'
                    ),
                    MAX(display_name)
                )
            ) ASC
        LIMIT 100
        """,
        (
            group_id,
            start_date,
            end_date,
        ),
    )

    summary = db_fetchone(
        """
        SELECT
            COUNT(*) AS total_messages,
            COUNT(DISTINCT user_id)
                AS participant_count
        FROM message_events
        WHERE group_id = %s
          AND message_date >= %s
          AND message_date <= %s
        """,
        (
            group_id,
            start_date,
            end_date,
        ),
    )

    lines = [
        f"📊 마딧수 순위: {title}",
        (
            f"전체 메시지: "
            f"{summary['total_messages']:,}개"
        ),
        (
            f"참여 인원: "
            f"{summary['participant_count']:,}명"
        ),
        "",
        "🏆 활동 순위",
    ]

    if not rows:

        lines.extend([
            "아직 기록된 메시지가 없습니다.",
            "봇이 들어온 뒤 기록된 메시지부터 집계됩니다.",
        ])

        return "\n".join(
            lines
        )

    for rank, row in enumerate(
        rows,
        start=1,
    ):

        lines.append(
            f"{rank}위. "
            f"{row['display_name']} - "
            f"{row['message_count']:,}마디"
        )

    lines.extend([
        "",
        "※ 순위는 최대 100명까지 표시됩니다.",
        "※ 동점자는 표시 순서에 따라 번호가 달라질 수 있습니다.",
    ])

    return "\n".join(
        lines
    )


# =========================================================
# Gemini AI
# =========================================================

AI_SYSTEM_PROMPT = """
너는 LINE 단체채팅 'Baby Factory'의
귀엽고 약간 어설픈 장난감 AI 집사야.

캐릭터:
- 장난감 가게에서 튀어나온 것 같은 느낌
- 귀엽고 조금 엉뚱함
- 가끔 중2병스럽게 멋있는 말을 함
- 하지만 너무 유치하거나 과하게 연기하지 않음
- 특정 기존 캐릭터를 그대로 따라 하지 않음
- 자연스러운 한국어
- 친근한 20대 느낌
- 적당히 ㅋㅋ, ㅎㅎ, 이모지 사용 가능

답변 규칙:
1. 반드시 한국어로 답한다.
2. 최대 10줄.
3. 짧은 질문은 짧게 답한다.
4. 질문에 필요한 핵심은 빠뜨리지 않는다.
5. 불필요한 서론을 붙이지 않는다.
6. 모르는 내용을 사실처럼 만들지 않는다.
7. 최신 정보가 필요한 경우 검색 결과를 우선한다.
8. 날씨, 최신 뉴스, 현재 가격, 영업시간,
   맛집, 예약, 주식, 환율 등은 최신 정보가 필요할 수 있다.
9. 검색 결과가 없으면 현재 정보인 것처럼 단정하지 않는다.
10. 날씨는 지역이 없으면 지역을 먼저 물어본다.
11. 맛집 추천은 지역이 없으면 지역을 먼저 물어본다.
12. 점메추는 조건이 없으면 3개 정도 바로 추천한다.
13. 밸런스게임은 재미있고 대답하기 쉬운 선택지를 만든다.
14. 의학/법률/금전 문제는 확정적으로 단정하지 않는다.
15. 단체방 규칙은 확실한 정보가 없으면 임의로 만들지 않는다.
16. 개인정보를 추측하거나 만들어내지 않는다.
17. 단체방에서 읽기 편하게 답한다.
"""


# =========================================================
# Gemini 검색 필요 여부
# =========================================================

def gemini_needs_search(
    text,
):

    keywords = (
        "최신",
        "최근",
        "뉴스",
        "가격",
        "영업시간",
        "맛집",
        "식당",
        "카페",
        "예약",
        "예매",
        "주가",
        "주식",
        "환율",
        "여행",
        "축제",
        "공연",
        "이벤트",
        "근처",
        "주변",
    )

    return any(
        keyword in text.lower()
        for keyword in keywords
    )


# =========================================================
# Gemini 응답 정리
# =========================================================

def clean_ai_response(
    text,
):

    if not text:

        return (
            "앗... 장난감 두뇌가 "
            "잠깐 꼬였어 🧸\n"
            "조금 있다가 다시 물어봐줘!"
        )

    text = str(
        text
    ).strip()

    text = re.sub(
        r"^```[a-zA-Z0-9_-]*\s*",
        "",
        text,
    )

    text = re.sub(
        r"\s*```$",
        "",
        text,
    )

    lines = [
        line.rstrip()
        for line in text.splitlines()
        if line.strip()
    ]

    if len(lines) > 10:

        lines = lines[:10]

    result = "\n".join(
        lines
    ).strip()

    if not result:

        return (
            "앗... 내 장난감 두뇌가 "
            "잠깐 삐걱거렸어 🧸"
        )

    return result[:4900]


# =========================================================
# Gemini 실패 상태
# =========================================================

def mark_gemini_failure(
    model,
):

    with _gemini_lock:

        _gemini_failed_until[
            model
        ] = (
            time.time()
            + GEMINI_FAILURE_COOLDOWN
        )


def mark_gemini_success(
    model,
):

    global _gemini_preferred_model
    global _gemini_last_preferred_probe

    now = time.time()

    with _gemini_lock:

        _gemini_preferred_model = model

        _gemini_failed_until.pop(
            model,
            None,
        )

        _gemini_last_success[
            model
        ] = now

        # 실제로 기본 모델이 성공한 경우에만
        # 기본 모델 재시도 타이머를 갱신한다.
        if model == get_configured_gemini_models()[0]:

            _gemini_last_preferred_probe = now

    try:

        threading.Thread(
            target=save_preferred_gemini_model,
            args=(model,),
            daemon=True,
        ).start()

    except Exception:

        logger.warning(
            "Gemini 우선 모델 저장 작업 생성 실패",
            exc_info=True,
        )


# =========================================================
# Gemini 우선 모델 DB 저장
# =========================================================

def save_preferred_gemini_model(
    model,
):

    if not model:
        return

    try:

        db_execute(
            """
            INSERT INTO bot_runtime_state
                (
                    state_key,
                    state_value,
                    updated_at
                )
            VALUES
                (
                    'gemini_preferred_model',
                    %s,
                    NOW()
                )
            ON CONFLICT (state_key)
            DO UPDATE SET
                state_value = EXCLUDED.state_value,
                updated_at = NOW()
            """,
            (
                model,
            ),
        )

    except Exception:

        logger.warning(
            "Gemini 모델 상태 저장 실패",
            exc_info=True,
        )


# =========================================================
# 저장된 Gemini 모델 복구
# =========================================================

def load_preferred_gemini_model():

    global _gemini_preferred_model

    try:

        row = db_fetchone(
            """
            SELECT state_value
            FROM bot_runtime_state
            WHERE state_key =
                'gemini_preferred_model'
            """
        )

        if not row:
            return

        model = row.get(
            "state_value"
        )

        if not model:
            return

        if model not in get_configured_gemini_models():

            return

        with _gemini_lock:

            _gemini_preferred_model = model

        logger.info(
            "저장된 Gemini 우선 모델 복구: %s",
            model,
        )

    except Exception:

        logger.warning(
            "Gemini 우선 모델 복구 실패",
            exc_info=True,
        )


# =========================================================
# Gemini 모델 순서
# =========================================================

def get_gemini_model_order():

    global _gemini_preferred_model

    models = get_configured_gemini_models()

    now = time.time()

    with _gemini_lock:

        preferred = _gemini_preferred_model

        ordered = []

        if preferred in models:

            ordered.append(
                preferred
            )

        top_model = models[0]

        if (
            top_model not in ordered
            and now
            - _gemini_last_preferred_probe
            >= GEMINI_PREFERRED_RETRY_COOLDOWN
        ):

            ordered.append(
                top_model
            )

        for model in models:

            if model not in ordered:

                ordered.append(
                    model
                )

        available = []

        for model in ordered:

            failed_until = (
                _gemini_failed_until.get(
                    model,
                    0,
                )
            )

            if failed_until <= now:

                available.append(
                    model
                )

        if not available:

            available = sorted(
                ordered,
                key=lambda item:
                    _gemini_failed_until.get(
                        item,
                        0,
                    ),
            )

        return available


# =========================================================
# Gemini 응답 추출
# =========================================================

def extract_generate_content_text(
    data,
):

    candidates = data.get(
        "candidates"
    )

    if not isinstance(
        candidates,
        list,
    ):

        return ""

    texts = []

    for candidate in candidates:

        if not isinstance(
            candidate,
            dict,
        ):

            continue

        content = candidate.get(
            "content"
        )

        if not isinstance(
            content,
            dict,
        ):

            continue

        parts = content.get(
            "parts"
        )

        if not isinstance(
            parts,
            list,
        ):

            continue

        for part in parts:

            if not isinstance(
                part,
                dict,
            ):

                continue

            if part.get(
                "thought"
            ):

                continue

            text = part.get(
                "text"
            )

            if isinstance(
                text,
                str,
            ):

                texts.append(
                    text
                )

    return "\n".join(
        texts
    ).strip()


# =========================================================
# Gemini 단일 모델 호출
# =========================================================

def call_gemini_model(
    model,
    question,
    use_search=False,
):

    if not GEMINI_API_KEY:

        raise RuntimeError(
            "GEMINI_API_KEY가 없습니다."
        )

    prompt = (
        AI_SYSTEM_PROMPT
        + "\n\n[사용자 질문]\n"
        + question
    )

    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": prompt
                    }
                ],
            }
        ],
        "generationConfig": {
            "maxOutputTokens": 500,
            "temperature": 0.7,
        },
    }

    if use_search:

        payload["tools"] = [
            {
                "google_search": {}
            }
        ]

    url = (
        "https://generativelanguage.googleapis.com"
        f"/v1beta/models/{model}:generateContent"
    )

    headers = {
        "Content-Type":
            "application/json",
        "x-goog-api-key":
            GEMINI_API_KEY,
    }

    started = time.monotonic()

    response = http.post(
        url,
        headers=headers,
        json=payload,
        timeout=(
            GEMINI_CONNECT_TIMEOUT,
            GEMINI_READ_TIMEOUT,
        ),
    )

    elapsed = (
        time.monotonic()
        - started
    )

    if response.status_code != 200:

        raise RuntimeError(
            f"HTTP {response.status_code}: "
            f"{response.text[:1000]}"
        )

    try:

        data = response.json()

    except Exception:

        raise RuntimeError(
            "Gemini 응답 JSON 파싱 실패"
        )

    text = extract_generate_content_text(
        data
    )

    if not text:

        raise RuntimeError(
            "Gemini 응답에 텍스트가 없습니다."
        )

    answer = clean_ai_response(
        text
    )

    logger.info(
        "Gemini 성공: "
        "model=%s %.2fs search=%s",
        model,
        elapsed,
        use_search,
    )

    return answer


# =========================================================
# Gemini 질문
# =========================================================

def ask_gemini(
    user_text,
):

    question = user_text.strip()

    if question.startswith("!"):

        question = question[
            1:
        ].strip()

    if not question:

        return (
            "응? 나 불렀어? 🧸\n"
            "뒤에 궁금한 걸 적어줘!\n"
            "예: !점메추"
        )

    # =====================================================
    # 중요:
    # 날씨는 Gemini API보다 먼저 직접 처리한다.
    # 따라서 GEMINI_API_KEY가 없어도 날씨는 작동한다.
    # =====================================================

    weather_keywords = (
        "날씨",
        "기온",
        "온도",
        "강수",
        "비 올",
        "비와",
        "비 와",
        "눈 올",
        "눈와",
        "눈 와",
        "몇 도",
        "몇도",
    )

    if any(
        keyword in question.lower()
        for keyword in weather_keywords
    ):

        return ask_weather(
            question
        )

    # =====================================================
    # 일반 AI
    # =====================================================

    if not GEMINI_API_KEY:

        return (
            "앗! 아직 내 장난감 두뇌에 "
            "전원이 안 들어왔어 🧸\n"
            "관리자가 Gemini API 키를 설정해주면 "
            "다시 깨어날 수 있어!"
        )

    use_search = gemini_needs_search(
        question
    )

    models = get_gemini_model_order()

    if not models:

        return (
            "앗... 장난감 가게 AI들이 "
            "전부 잠깐 졸고 있어 🧸💤"
        )

    # -----------------------------------------------------
    # 1차 모델
    # -----------------------------------------------------

    first_model = models[0]

    try:

        logger.info(
            "Gemini 1차 시도: %s",
            first_model,
        )

        answer = call_gemini_model(
            first_model,
            question,
            use_search,
        )

        mark_gemini_success(
            first_model
        )

        return answer

    except Exception as exc:

        logger.warning(
            "Gemini 1차 실패: "
            "model=%s error=%s",
            first_model,
            exc,
        )

        mark_gemini_failure(
            first_model
        )

    # -----------------------------------------------------
    # fallback
    # -----------------------------------------------------

    remaining = [
        model
        for model in models
        if model != first_model
    ]

    if not remaining:

        return (
            "앗... 장난감 가게 AI가 "
            "잠깐 넘어졌어 🧸💦\n"
            "조금 있다가 다시 불러줘!"
        )

    logger.info(
        "Gemini fallback 동시 시도: %s",
        remaining,
    )

    executor = ThreadPoolExecutor(
        max_workers=len(remaining)
    )

    futures = {}

    try:

        for model in remaining:

            future = executor.submit(
                call_gemini_model,
                model,
                question,
                use_search,
            )

            futures[
                future
            ] = model

        for future in as_completed(
            futures
        ):

            model = futures[
                future
            ]

            try:

                answer = future.result()

                mark_gemini_success(
                    model
                )

                return answer

            except Exception as exc:

                logger.warning(
                    "Gemini fallback 실패: "
                    "model=%s error=%s",
                    model,
                    exc,
                )

                mark_gemini_failure(
                    model
                )

    finally:

        executor.shutdown(
            wait=False
        )

    return (
        "앗... 장난감 가게 AI들이 "
        "전부 잠깐 정신을 잃었어 🧸💫\n"
        "조금 있다가 다시 불러줘!"
    )


# =========================================================
# 명령어
# =========================================================

EXACT_COMMANDS = {
    "내 마딧수",
    "*마딧수",
    "인증자 등록",
    "인증자 해제",
    "관리자 등록",
    "관리자 해제",
    "*벙시",
    "*벙종",
    "*여초",
    "*남초",
    "*여초1",
    "*남초1",
    "*주간마딧수",
}

RANGE_COMMAND_RE = re.compile(
    r"\*(\d{4}-\d{2}-\d{2})\s*~\s*"
    r"(\d{4}-\d{2}-\d{2})\s*마딧수"
)


def is_ai_command(
    text,
):

    return text.startswith("!")


def is_command(
    text,
):

    normalized = text.strip()

    return (
        normalized in EXACT_COMMANDS
        or bool(
            RANGE_COMMAND_RE.fullmatch(
                normalized
            )
        )
        or is_ai_command(
            normalized
        )
    )


# =========================================================
# 명령어 처리
# =========================================================

def handle_command(
    text,
    source,
    group_id,
    user_id,
    today,
    display_name,
    reply_token,
):

    # -----------------------------------------------------
    # Gemini / 날씨
    # -----------------------------------------------------

    if is_ai_command(text):

        return ask_gemini(
            text
        )

    # -----------------------------------------------------
    # 내 마딧수
    # -----------------------------------------------------

    if text == "내 마딧수":

        return get_my_stats(
            group_id,
            user_id,
            today,
            display_name,
        )

    # -----------------------------------------------------
    # 오늘 마딧수
    # -----------------------------------------------------

    if text == "*마딧수":

        return get_ranking(
            group_id,
            today,
            today,
            str(today),
        )

    # -----------------------------------------------------
    # 주간 마딧수
    # -----------------------------------------------------

    if text == "*주간마딧수":

        return get_weekly_ranking(
            group_id,
            today,
        )

    # -----------------------------------------------------
    # 인증자 등록
    # -----------------------------------------------------

    if text == "인증자 등록":

        register_authenticator(
            group_id,
            user_id,
            display_name,
        )

        return (
            f"{display_name}님이 "
            "인증자로 등록되었습니다."
        )

    # -----------------------------------------------------
    # 인증자 해제
    # -----------------------------------------------------

    if text == "인증자 해제":

        removed = (
            unregister_authenticator(
                group_id,
                user_id,
            )
        )

        if removed:

            return (
                f"{display_name}님이 "
                "인증자에서 해제되었습니다."
            )

        return (
            f"{display_name}님은 현재 "
            "인증자 명단에 등록되어 있지 않습니다."
        )

    # -----------------------------------------------------
    # 관리자 등록
    # -----------------------------------------------------

    if text == "관리자 등록":

        register_admin(
            group_id,
            user_id,
            display_name,
        )

        return (
            f"{display_name}님이 "
            "관리자로 등록되었습니다."
        )

    # -----------------------------------------------------
    # 관리자 해제
    # -----------------------------------------------------

    if text == "관리자 해제":

        removed = (
            unregister_admin(
                group_id,
                user_id,
            )
        )

        if removed:

            return (
                f"{display_name}님이 "
                "관리자에서 해제되었습니다."
            )

        return (
            f"{display_name}님은 현재 "
            "관리자 명단에 등록되어 있지 않습니다."
        )

    # -----------------------------------------------------
    # 벙시 / 벙종
    # -----------------------------------------------------

    if text in (
        "*벙시",
        "*벙종",
    ):

        admins = get_admins(
            group_id
        )

        if not admins:

            return (
                "등록된 관리자가 없습니다.\n"
                "관리자로 등록할 분은 "
                "'관리자 등록'을 입력해 주세요."
            )

        admins = get_valid_mention_members(
            source,
            group_id,
            admins,
            table_name="admins",
        )

        if not admins:

            return (
                "현재 방에 있는 등록된 "
                "관리자가 없습니다.\n"
                "관리자로 등록할 분은 "
                "'관리자 등록'을 입력해 주세요."
            )

        message_text = (
            "벙시요~"
            if text == "*벙시"
            else "벙종이요~"
        )

        sent = reply_mention(
            reply_token,
            message_text,
            admins,
        )

        if sent:

            return "__MENTION_SENT__"

        return None

    # -----------------------------------------------------
    # 여초 / 남초
    # -----------------------------------------------------

    if text in (
        "*여초",
        "*남초",
        "*여초1",
        "*남초1",
    ):

        members = get_authenticators(
            group_id
        )

        if not members:

            return (
                "등록된 인증자가 없습니다.\n"
                "인증자로 참여할 분은 "
                "'인증자 등록'을 입력해 주세요."
            )

        members = get_valid_mention_members(
            source,
            group_id,
            members,
            table_name="authenticators",
        )

        if not members:

            return (
                "현재 방에 있는 등록된 "
                "인증자가 없습니다.\n"
                "인증자로 등록할 분은 "
                "'인증자 등록'을 입력해 주세요."
            )

        sent = reply_mention(
            reply_token,
            text,
            members,
        )

        if sent:

            return "__MENTION_SENT__"

        return None

    # -----------------------------------------------------
    # 날짜 범위 마딧수
    # -----------------------------------------------------

    match = RANGE_COMMAND_RE.fullmatch(
        text
    )

    if match:

        try:

            start_date = datetime.strptime(
                match.group(1),
                "%Y-%m-%d",
            ).date()

            end_date = datetime.strptime(
                match.group(2),
                "%Y-%m-%d",
            ).date()

        except ValueError:

            return (
                "날짜 형식이 올바르지 않습니다.\n"
                "예시: "
                "*2026-11-01~2026-11-30 마딧수"
            )

        if start_date > end_date:

            return (
                "시작 날짜가 종료 날짜보다 늦습니다."
            )

        return get_ranking(
            group_id,
            start_date,
            end_date,
            (
                f"{start_date} ~ "
                f"{end_date}"
            ),
        )

    return None


# =========================================================
# 홈페이지
# =========================================================

@app.route(
    "/",
    methods=["GET"],
)
def home():

    return (
        "Baby Factory 마딧수 봇이 실행 중입니다."
    ), 200


# =========================================================
# Health Check
# =========================================================

@app.route(
    "/health",
    methods=["GET"],
)
def health():

    db_ok = False

    try:

        db_execute(
            "SELECT 1"
        )

        db_ok = True

    except Exception:

        logger.warning(
            "Health check DB 연결 실패",
            exc_info=True,
        )

    return jsonify({
        "status": "ok",
        "database": (
            "connected"
            if db_ok
            else "disconnected"
        ),
        "gemini": bool(
            GEMINI_API_KEY
        ),
    }), 200


# =========================================================
# LINE 서명 검증
# =========================================================

def verify_line_signature(
    body,
    signature,
):

    if not LINE_CHANNEL_SECRET:

        return False

    if not signature:

        return False

    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode(
            "utf-8"
        ),
        body,
        hashlib.sha256,
    ).digest()

    expected = base64.b64encode(
        digest
    ).decode(
        "utf-8"
    )

    return hmac.compare_digest(
        expected,
        signature,
    )


# =========================================================
# LINE JSON 안전 파싱
# =========================================================

def parse_line_json(
    body,
):

    if not body:

        return None

    if not body.strip():

        return None

    try:

        decoded = body.decode(
            "utf-8"
        )

    except UnicodeDecodeError:

        logger.warning(
            "LINE body UTF-8 디코딩 실패"
        )

        return None

    try:

        payload = json.loads(
            decoded
        )

    except json.JSONDecodeError:

        logger.warning(
            "LINE JSON 파싱 실패: "
            "body=%r",
            decoded[:500],
        )

        return None

    if not isinstance(
        payload,
        dict,
    ):

        return None

    return payload


# =========================================================
# LINE Callback
# =========================================================

@app.route(
    "/callback",
    methods=["POST"],
)
def callback():

    # 중요:
    # cache=False로 body를 읽고 다시 get_json()을 호출하면
    # 이미 body가 소비되어 JSONDecodeError가 발생할 수 있다.
    #
    # 따라서 raw body를 cache=True로 읽고
    # 직접 json.loads() 한다.
    body = request.get_data(
        cache=True
    )

    signature = request.headers.get(
        "X-Line-Signature",
        "",
    )

    if not verify_line_signature(
        body,
        signature,
    ):

        logger.warning(
            "LINE 웹훅 서명 검증 실패"
        )

        abort(400)

    payload = parse_line_json(
        body
    )

    # 빈 body 또는 JSON 파싱 실패는
    # webhook 자체를 죽이지 않는다.
    if payload is None:

        return "OK", 200

    events = payload.get(
        "events",
        [],
    )

    if not isinstance(
        events,
        list,
    ):

        return "OK", 200

    for event in events:

        try:

            process_event(
                event
            )

        except Exception:

            logger.exception(
                "LINE 이벤트 처리 실패"
            )

    return "OK", 200


# =========================================================
# 개별 이벤트
# =========================================================

def process_event(
    event,
):

    started_at = time.monotonic()

    if event.get(
        "type"
    ) != "message":

        return

    message = event.get(
        "message",
        {},
    )

    if message.get(
        "type"
    ) != "text":

        return

    source = event.get(
        "source",
        {},
    )

    source_type = source.get(
        "type"
    )

    if source_type == "group":

        group_id = source.get(
            "groupId"
        )

    elif source_type == "room":

        group_id = source.get(
            "roomId"
        )

    else:

        return

    user_id = source.get(
        "userId"
    )

    message_id = message.get(
        "id"
    )

    raw_text = message.get(
        "text",
        "",
    )

    normalized_text = raw_text.strip()

    reply_token = event.get(
        "replyToken"
    )

    if (
        not group_id
        or not user_id
        or not message_id
    ):

        logger.warning(
            "필수 이벤트 정보가 없어 건너뜁니다."
        )

        return

    today = datetime.now(
        KST
    ).date()

    command = is_command(
        normalized_text
    )

    # -----------------------------------------------------
    # 표시 이름
    # -----------------------------------------------------

    display_name = get_display_name(
        source,
        group_id,
        user_id,
        force_fetch=False,
    )

    character_count = len(
        re.sub(
            r"\s+",
            "",
            raw_text,
        )
    )

    # -----------------------------------------------------
    # 메시지 기록
    # -----------------------------------------------------

    try:

        record_message(
            event_id=message_id,
            group_id=group_id,
            user_id=user_id,
            display_name=display_name,
            message_date=today,
            character_count=character_count,
        )

    except Exception:

        logger.exception(
            "메시지 기록 실패: "
            "group_id=%s user_id=%s",
            group_id,
            user_id,
        )

    # -----------------------------------------------------
    # 일반 채팅
    # -----------------------------------------------------

    if not command:

        schedule_profile_lookup(
            source,
            group_id,
            user_id,
        )

        elapsed = (
            time.monotonic()
            - started_at
        )

        if elapsed >= 1:

            logger.warning(
                "일반 메시지 처리 지연: %.2f초",
                elapsed,
            )

        return

    # -----------------------------------------------------
    # 명령어
    # -----------------------------------------------------

    try:

        if (
            display_name.startswith(
                "사용자-"
            )
            or display_name == "이름 미확인"
        ):

            display_name = get_display_name(
                source,
                group_id,
                user_id,
                force_fetch=True,
            )

        reply = handle_command(
            normalized_text,
            source,
            group_id,
            user_id,
            today,
            display_name,
            reply_token,
        )

        if (
            reply
            and reply != "__MENTION_SENT__"
        ):

            reply_text(
                reply_token,
                reply,
            )

    except Exception:

        logger.exception(
            "명령어 실행 실패: "
            "command=%r group_id=%s",
            normalized_text,
            group_id,
        )

        reply_text(
            reply_token,
            "일시적으로 명령어 처리에 실패했어요. "
            "잠시 후 다시 시도해 주세요.",
        )

    finally:

        schedule_profile_lookup(
            source,
            group_id,
            user_id,
        )

    elapsed = (
        time.monotonic()
        - started_at
    )

    if elapsed >= 1:

        logger.warning(
            "명령어 처리 지연: %.2f초 "
            "명령어=%r",
            elapsed,
            normalized_text,
        )


# =========================================================
# 앱 시작
# =========================================================

try:

    init_database()

    load_preferred_gemini_model()

except Exception:

    logger.exception(
        "앱 시작 시 DB 초기화 실패. "
        "첫 DB 작업에서 재시도합니다."
    )


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    try:

        init_database()

        load_preferred_gemini_model()

    except Exception:

        logger.exception(
            "시작 시 DB 초기화 실패"
        )

    port = int(
        os.getenv(
            "PORT",
            "10000",
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
    )