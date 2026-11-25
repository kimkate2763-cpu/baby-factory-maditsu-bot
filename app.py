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

# 선택사항:
# Render 환경변수 GEMINI_MODELS에
# gemini-3.8-flash,gemini-3.6-flash,gemini-3.5-flash-lite
# 처럼 넣으면 이 목록을 우선 사용한다.
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
        f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}ms'"
    )

    conn.commit()

    _db_connection = conn

    _db_connection_created_at = time.monotonic()

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

def _is_connection_error(exc):

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

                    if _is_connection_error(exc):

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
                and _is_connection_error(exc)
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

        # 네트워크 오류는 탈퇴로 간주하지 않는다.
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
            "user_id