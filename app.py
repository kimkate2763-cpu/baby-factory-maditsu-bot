import os
import hmac
import hashlib
import base64
import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
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
# Gemini 설정
# =========================================================

DEFAULT_GEMINI_MODELS = [
    "gemini-3.8-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash-lite",
]

GEMINI_FAILURE_COOLDOWN = 60

GEMINI_CONNECT_TIMEOUT = 1.2

GEMINI_READ_TIMEOUT = 7.0

_gemini_lock = threading.RLock()

_gemini_preferred_model = None

_gemini_failed_until = {}


def get_configured_gemini_models():
    models = []

    for model in (
        [GEMINI_MODEL]
        + GEMINI_MODELS_ENV.split(",")
        + DEFAULT_GEMINI_MODELS
    ):
        model = model.strip()

        if model and model not in models:
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

_db_init_lock = threading.Lock()

_db_connection = None

_db_connection_created_at = 0.0

_db_initialized = False

DB_CONNECT_TIMEOUT = 5

DB_RECONNECT_AFTER = 1800

DB_STATEMENT_TIMEOUT = 8000


# =========================================================
# 프로필 캐시
# =========================================================

_PROFILE_CACHE = {}

_PROFILE_CACHE_LOCK = threading.Lock()

_PROFILE_CACHE_TTL = 600

_PROFILE_LOOKUPS_IN_FLIGHT = set()

_PROFILE_EXECUTOR = ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="line-profile",
)


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

    return conn


# =========================================================
# DB 연결 가져오기
# =========================================================

def _get_connection_locked():

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

    keywords = (
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
        for keyword in keywords
    )


# =========================================================
# DB 초기화
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

    raise (
        last_error
        or RuntimeError("DB 작업 실패")
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
# 프로필 URL
# =========================================================

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
# 실제 이름 DB 저장
# =========================================================

def _save_profile_name(
    group_id,
    user_id,
    name,
):

    for table in (
        "message_events",
        "group_authenticators",
        "group_admins",
    ):

        db_execute(
            f"""
            UPDATE {table}
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
# LINE 프로필 조회
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

        if response.status_code != 200:
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

            logger.warning(
                "프로필 이름 DB 갱신 실패",
                exc_info=True,
            )

        return name

    except Exception:

        logger.warning(
            "프로필 조회 실패",
            exc_info=True,
        )

        return None

    finally:

        with _PROFILE_CACHE_LOCK:

            _PROFILE_LOOKUPS_IN_FLIGHT.discard(
                cache_key
            )


# =========================================================
# 최근 이름 조회
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

        pass

    return None


# =========================================================
# 표시 이름
# =========================================================

def get_display_name(
    source,
    group_id,
    user_id,
    *,
    force_fetch=False,
):

    fallback = (
        "사용자-"
        + user_id[-6:]
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
            and not force_fetch
        ):

            return cached[1]

    recent_name = get_recent_display_name(
        group_id,
        user_id,
    )

    if (
        recent_name
        and not force_fetch
    ):

        with _PROFILE_CACHE_LOCK:

            _PROFILE_CACHE[
                cache_key
            ] = (
                time.time(),
                recent_name,
            )

        return recent_name

    if force_fetch:

        fetched_name = _fetch_profile_name(
            source,
            group_id,
            user_id,
            cache_key,
        )

        if fetched_name:

            return fetched_name

    return (
        recent_name
        or fallback
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

        if (
            cache_key
            in _PROFILE_LOOKUPS_IN_FLIGHT
        ):

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


# =========================================================
# 일반 LINE 답장
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
                "replyToken":
                    reply_token,
                "messages": [
                    {
                        "type": "text",
                        "text":
                            str(text)[:4900],
                    }
                ],
            },
            timeout=(2.5, 6),
        )

        if not response.ok:

            logger.error(
                "LINE 답장 실패: "
                "status=%s body=%s",
                response.status_code,
                response.text[:500],
            )

            return False

        return True

    except Exception:

        logger.exception(
            "LINE 답장 전송 오류"
        )

        return False


# =========================================================
# 그룹 멤버 확인
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
            return False

        return None

    except Exception:

        return None


# =========================================================
# 유효 멘션 멤버
# =========================================================

def get_valid_mention_members(
    source,
    group_id,
    members,
    *,
    table_name,
):

    valid = []

    stale = []

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

            valid.append(
                member
            )

        elif status is False:

            stale.append(
                user_id
            )

    for user_id in stale:

        table = (
            "group_admins"
            if table_name == "admins"
            else "group_authenticators"
        )

        try:

            db_execute(
                f"""
                DELETE FROM {table}
                WHERE group_id = %s
                  AND user_id = %s
                """,
                (
                    group_id,
                    user_id,
                ),
            )

        except Exception:

            pass

    return valid


# =========================================================
# LINE 멘션
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

    valid_members = [
        member
        for member in members
        if str(
            member.get("user_id", "")
        ).startswith("U")
    ][:20]

    if not valid_members:

        return reply_text(
            reply_token,
            text,
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
            "type":
                "mention",
            "mentionee": {
                "type":
                    "user",
                "userId":
                    member["user_id"],
            },
        }

    message_text = (
        str(text).rstrip()
        + "\n"
        + " ".join(placeholders)
    )

    try:

        response = http.post(
            "https://api.line.me/v2/bot/message/reply",
            headers=line_headers(),
            json={
                "replyToken":
                    reply_token,
                "messages": [
                    {
                        "type":
                            "textV2",
                        "text":
                            message_text[:4900],
                        "substitution":
                            substitution,
                    }
                ],
            },
            timeout=(2.5, 6),
        )

        if not response.ok:

            logger.error(
                "LINE 멘션 실패: "
                "status=%s body=%s",
                response.status_code,
                response.text[:700],
            )

            return False

        return True

    except Exception:

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
            display_name =
                EXCLUDED.display_name,
            registered_at =
                NOW()
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
            display_name =
                EXCLUDED.display_name,
            registered_at =
                NOW()
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
        get_week_start(
            target_date
        )
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
# 마딧수 순위
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
                    AND display_name <>
                        '이름 미확인'
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
                        AND display_name <>
                            '이름 미확인'
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
# 주간 순위
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
                    AND display_name <>
                        '이름 미확인'
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
                        AND display_name <>
                            '이름 미확인'
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
        f"기간: {week_start} ~ {week_end}",
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
# 날씨 설정
# =========================================================

WEATHER_GEOCODING_URL = (
    "https://geocoding-api.open-meteo.com/v1/search"
)

WEATHER_FORECAST_URL = (
    "https://api.open-meteo.com/v1/forecast"
)

WEATHER_CONNECT_TIMEOUT = 1.5

WEATHER_READ_TIMEOUT = 5.0


# =========================================================
# 한국 지역 별칭
# =========================================================

WEATHER_ALIASES = {

    "서울":
        (37.5665, 126.9780, "서울"),

    "서울시":
        (37.5665, 126.9780, "서울"),

    "부산":
        (35.1796, 129.0756, "부산"),

    "대구":
        (35.8714, 128.6014, "대구"),

    "인천":
        (37.4563, 126.7052, "인천"),

    "광주":
        (35.1595, 126.8526, "광주"),

    "대전":
        (36.3504, 127.3845, "대전"),

    "울산":
        (35.5384, 129.3114, "울산"),

    "세종":
        (36.4800, 127.2890, "세종"),

    "제주":
        (33.4996, 126.5312, "제주"),

    "마포":
        (37.5663, 126.9014, "마포구"),

    "마포구":
        (37.5663, 126.9014, "마포구"),

    "홍대":
        (37.5563, 126.9236, "홍대"),

    "홍대입구":
        (37.5572, 126.9245, "홍대입구"),

    "혜화":
        (37.5822, 127.0018, "혜화"),

    "종로":
        (37.5730, 126.9794, "종로구"),

    "종로구":
        (37.5730, 126.9794, "종로구"),

    "강남":
        (37.4979, 127.0276, "강남"),

    "강남구":
        (37.5172, 127.0473, "강남구"),

    "잠실":
        (37.5133, 127.1000, "잠실"),

    "성수":
        (37.5446, 127.0559, "성수"),

    "여의도":
        (37.5219, 126.9245, "여의도"),

    "건대":
        (37.5404, 127.0693, "건대"),

    "신촌":
        (37.5596, 126.9423, "신촌"),
}


# =========================================================
# WMO 날씨 코드
# =========================================================

WEATHER_CODE_TEXT = {

    0: "맑음",

    1: "대체로 맑음",

    2: "부분적으로 흐림",

    3: "흐림",

    45: "안개",

    48: "짙은 안개",

    51: "약한 이슬비",

    53: "이슬비",

    55: "강한 이슬비",

    56: "약한 어는 이슬비",

    57: "강한 어는 이슬비",

    61: "약한 비",

    63: "비",

    65: "강한 비",

    66: "약한 얼어붙는 비",

    67: "강한 얼어붙는 비",

    71: "약한 눈",

    73: "눈",

    75: "강한 눈",

    77: "눈알갱이",

    80: "약한 소나기",

    81: "소나기",

    82: "강한 소나기",

    85: "약한 눈 소나기",

    86: "강한 눈 소나기",

    95: "뇌우",

    96: "약한 우박 동반 뇌우",

    99: "강한 우박 동반 뇌우",
}


# =========================================================
# 날씨 질문 여부
# =========================================================

def is_weather_query(
    text,
):

    text = text.lower()

    keywords = (
        "날씨",
        "기온",
        "기상",
        "비 와",
        "비와",
        "눈 와",
        "눈와",
        "우산",
        "강수",
    )

    return any(
        keyword in text
        for keyword in keywords
    )


# =========================================================
# 오늘 / 내일 / 모레
# =========================================================

def extract_weather_day(
    text,
):

    text = text.lower()

    if "모레" in text:
        return 2

    if "내일" in text:
        return 1

    return 0


# =========================================================
# 날씨 지역 추출
# =========================================================

def extract_weather_location(
    text,
):

    text = re.sub(
        r"^\s*!\s*",
        "",
        text,
    ).strip()

    # 먼저 확실한 별칭부터 찾는다.
    # 서울 / 혜화 / 홍대 등이 절대 Gemini에 넘어가지 않는다.

    for alias in sorted(
        WEATHER_ALIASES,
        key=len,
        reverse=True,
    ):

        if alias in text:

            return alias

    text = re.sub(
        r"\b(?:오늘|내일|모레|지금|현재)\b",
        " ",
        text,
    )

    text = re.sub(
        r"(?:날씨|기온|기상|강수|"
        r"비\s*와(?:요)?|"
        r"눈\s*와(?:요)?|"
        r"우산|"
        r"알려줘|"
        r"알려\s*줘|"
        r"보여줘|"
        r"봐줘|"
        r"궁금해|"
        r"어때(?:요)?|"
        r"어떠냐|"
        r"어떤가요?)",
        " ",
        text,
        flags=re.I,
    )

    text = re.sub(
        r"[^0-9A-Za-z가-힣·\-\s]",
        " ",
        text,
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    ).strip()

    if not text:
        return None

    parts = text.split()

    return parts[-1]


# =========================================================
# 지역 검색
# =========================================================

def geocode_weather_location(
    location,
):

    if not location:
        return None

    location = location.strip()

    if location in WEATHER_ALIASES:

        lat, lon, name = (
            WEATHER_ALIASES[
                location
            ]
        )

        return {
            "latitude":
                lat,
            "longitude":
                lon,
            "name":
                name,
        }

    try:

        response = http.get(
            WEATHER_GEOCODING_URL,
            params={
                "name":
                    location,
                "count":
                    5,
                "language":
                    "ko",
                "format":
                    "json",
            },
            timeout=(
                WEATHER_CONNECT_TIMEOUT,
                WEATHER_READ_TIMEOUT,
            ),
        )

        if not response.ok:

            return None

        results = (
            response.json()
            .get("results")
            or []
        )

        if not results:
            return None

        results.sort(
            key=lambda item: (
                0
                if item.get(
                    "country_code"
                ) == "KR"
                else 1,
                -float(
                    item.get(
                        "population"
                    )
                    or 0
                ),
            )
        )

        item = results[0]

        return {
            "latitude":
                item["latitude"],
            "longitude":
                item["longitude"],
            "name":
                item.get(
                    "name"
                )
                or location,
        }

    except Exception:

        logger.warning(
            "날씨 지역 검색 실패: %s",
            location,
            exc_info=True,
        )

        return None


# =========================================================
# 날씨 아이콘
# =========================================================

def weather_icon(
    code,
):

    if code == 0:
        return "☀️"

    if code in (1, 2):
        return "🌤️"

    if code == 3:
        return "☁️"

    if code in (45, 48):
        return "🌫️"

    if code in (
        51,
        53,
        55,
        56,
        57,
    ):
        return "🌦️"

    if code in (
        61,
        63,
        65,
        66,
        67,
        80,
        81,
        82,
    ):
        return "🌧️"

    if code in (
        71,
        73,
        75,
        77,
        85,
        86,
    ):
        return "❄️"

    if code in (
        95,
        96,
        99,
    ):
        return "⛈️"

    return "🌤️"


# =========================================================
# 실제 날씨 조회
# =========================================================

def get_weather_answer(
    question,
):

    if not is_weather_query(
        question
    ):

        return None

    location_text = (
        extract_weather_location(
            question
        )
    )

    if not location_text:

        return (
            "어느 지역 날씨 볼까? 🧸☁️\n"
            "예: !서울 날씨 / "
            "!혜화 날씨 / "
            "!마포구 날씨"
        )

    place = geocode_weather_location(
        location_text
    )

    if not place:

        return (
            f"'{location_text}' 지역을 "
            "찾지 못했어 🧸💦\n"
            "정확한 지역명을 적어줘!\n"
            "예: !서울 날씨 / "
            "!종로구 날씨"
        )

    day_offset = extract_weather_day(
        question
    )

    try:

        params = {
            "latitude":
                place["latitude"],

            "longitude":
                place["longitude"],

            "current":
                "temperature_2m,"
                "relative_humidity_2m,"
                "apparent_temperature,"
                "precipitation,"
                "weather_code,"
                "wind_speed_10m",

            "daily":
                "weather_code,"
                "temperature_2m_max,"
                "temperature_2m_min,"
                "precipitation_probability_max,"
                "precipitation_sum",

            "forecast_days":
                3,

            "timezone":
                "Asia/Seoul",
        }

        response = http.get(
            WEATHER_FORECAST_URL,
            params=params,
            timeout=(
                WEATHER_CONNECT_TIMEOUT,
                WEATHER_READ_TIMEOUT,
            ),
        )

        if not response.ok:

            raise RuntimeError(
                f"weather HTTP "
                f"{response.status_code}"
            )

        data = response.json()

        daily = (
            data.get("daily")
            or {}
        )

        codes = (
            daily.get(
                "weather_code"
            )
            or []
        )

        highs = (
            daily.get(
                "temperature_2m_max"
            )
            or []
        )

        lows = (
            daily.get(
                "temperature_2m_min"
            )
            or []
        )

        rain_probs = (
            daily.get(
                "precipitation_probability_max"
            )
            or []
        )

        rain_sums = (
            daily.get(
                "precipitation_sum"
            )
            or []
        )

        code = int(
            codes[
                day_offset
            ]
        )

        high = float(
            highs[
                day_offset
            ]
        )

        low = float(
            lows[
                day_offset
            ]
        )

        rain_prob = int(
            rain_probs[
                day_offset
            ]
            or 0
        )

        rain_sum = float(
            rain_sums[
                day_offset
            ]
            or 0
        )

        day_name = (
            "오늘",
            "내일",
            "모레",
        )[day_offset]

        lines = [
            (
                f"{weather_icon(code)} "
                f"{place['name']} "
                f"{day_name} 날씨"
            ),
            (
                "상태: "
                + WEATHER_CODE_TEXT.get(
                    code,
                    "날씨 정보 확인 필요",
                )
            ),
            (
                f"최고 {high:.1f}°C / "
                f"최저 {low:.1f}°C"
            ),
            (
                f"강수확률 최대 "
                f"{rain_prob}% / "
                f"예상 강수량 "
                f"{rain_sum:.1f}mm"
            ),
        ]

        if (
            day_offset == 0
            and data.get("current")
        ):

            current = data[
                "current"
            ]

            temp = float(
                current.get(
                    "temperature_2m"
                )
            )

            feel = float(
                current.get(
                    "apparent_temperature"
                )
            )

            wind = float(
                current.get(
                    "wind_speed_10m"
                )
            )

            lines.insert(
                2,
                (
                    f"현재 {temp:.1f}°C / "
                    f"체감 {feel:.1f}°C"
                ),
            )

            lines.append(
                f"💨 바람 {wind:.1f}m/s"
            )

        return "\n".join(
            lines
        )

    except Exception:

        logger.warning(
            "날씨 조회 실패",
            exc_info=True,
        )

        return (
            "날씨 서버에서 정보를 "
            "받아오지 못했어 🧸💦\n"
            "잠시 후 다시 물어봐줘!"
        )


# =========================================================
# Gemini AI
# =========================================================

AI_SYSTEM_PROMPT = """
너는 LINE 단체채팅 'Baby Factory'의
귀엽고 약간 어설픈 장난감 AI 집사야.

캐릭터:
- 장난감 가게에서 튀어나온 느낌
- 귀엽고 조금 엉뚱함
- 가끔 중2병스럽게 멋있는 말을 함
- 너무 유치하게 연기하지 않음
- 자연스러운 한국어
- 친근한 20대 느낌
- ㅋㅋ, ㅎㅎ, 이모지는 적당히 사용

답변 규칙:
1. 반드시 한국어로 답한다.
2. 최대 10줄.
3. 짧은 질문은 짧게 답한다.
4. 필요한 핵심은 빠뜨리지 않는다.
5. 불필요한 서론을 붙이지 않는다.
6. 모르는 내용을 사실처럼 만들지 않는다.
7. 최신 정보가 필요한 경우 검색 결과를 우선한다.
8. 단체방 규칙은 확실한 정보가 없으면 임의로 만들지 않는다.
9. 개인정보를 추측하거나 만들어내지 않는다.
10. 단체방에서 읽기 편하게 답한다.
"""


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


# =========================================================
# Gemini 모델 순서
# =========================================================

def get_gemini_model_order():

    models = get_configured_gemini_models()

    now = time.time()

    with _gemini_lock:

        available = [
            model
            for model in models
            if (
                _gemini_failed_until.get(
                    model,
                    0,
                )
                <= now
            )
        ]

        if available:

            return available

        return models


# =========================================================
# Gemini 응답 텍스트 추출
# =========================================================

def extract_generate_content_text(
    data,
):

    texts = []

    for candidate in (
        data.get(
            "candidates"
        )
        or []
    ):

        content = (
            candidate.get(
                "content"
            )
            or {}
        )

        for part in (
            content.get(
                "parts"
            )
            or []
        ):

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
        "현재",
    )

    return any(
        keyword in text.lower()
        for keyword in keywords
    )


# =========================================================
# Gemini 단일 호출
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
                "role":
                    "user",
                "parts": [
                    {
                        "text":
                            prompt,
                    }
                ],
            }
        ],
        "generationConfig": {
            "maxOutputTokens":
                500,
            "temperature":
                0.7,
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

    response = http.post(
        url,
        headers={
            "Content-Type":
                "application/json",
            "x-goog-api-key":
                GEMINI_API_KEY,
        },
        json=payload,
        timeout=(
            GEMINI_CONNECT_TIMEOUT,
            GEMINI_READ_TIMEOUT,
        ),
    )

    if response.status_code != 200:

        raise RuntimeError(
            f"HTTP {response.status_code}: "
            f"{response.text[:700]}"
        )

    data = response.json()

    text = extract_generate_content_text(
        data
    )

    if not text:

        raise RuntimeError(
            "Gemini 응답에 텍스트가 없습니다."
        )

    return clean_ai_response(
        text
    )


# =========================================================
# Gemini 질문
# =========================================================

def ask_gemini(
    user_text,
):

    question = (
        user_text.strip()[1:].strip()
        if user_text.strip().startswith("!")
        else user_text.strip()
    )

    if not question:

        return (
            "응? 나 불렀어? 🧸\n"
            "뒤에 궁금한 걸 적어줘!\n"
            "예: !점메추"
        )

    # =====================================================
    # 중요
    # 날씨는 Gemini 검색을 사용하지 않는다.
    # Open-Meteo에서 직접 조회한다.
    # =====================================================

    if is_weather_query(
        question
    ):

        return get_weather_answer(
            question
        )

    # =====================================================
    # 일반 AI
    # =====================================================

    if not GEMINI_API_KEY:

        return (
            "앗! 아직 내 장난감 두뇌에 "
            "전원이 안 들어왔어 🧸\n"
            "관리자가 Gemini API 키를 "
            "설정해주면 다시 깨어날 수 있어!"
        )

    use_search = gemini_needs_search(
        question
    )

    models = get_gemini_model_order()

    errors = []

    # =====================================================
    # 모델 순차 fallback
    # =====================================================

    for model in models:

        try:

            answer = call_gemini_model(
                model,
                question,
                use_search,
            )

            logger.info(
                "Gemini 성공: "
                "model=%s search=%s",
                model,
                use_search,
            )

            return answer

        except Exception as exc:

            errors.append(
                f"{model}: {exc}"
            )

            logger.warning(
                "Gemini 실패: "
                "model=%s error=%s",
                model,
                exc,
            )

            mark_gemini_failure(
                model
            )

    logger.error(
        "모든 Gemini 모델 실패: %s",
        " | ".join(errors)[:2000],
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
    "*주간마딧수",
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
}


RANGE_COMMAND_RE = re.compile(
    r"\*(\d{4}-\d{2}-\d{2})"
    r"\s*~\s*"
    r"(\d{4}-\d{2}-\d{2})"
    r"\s*마딧수"
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
    # AI
    # -----------------------------------------------------

    if is_ai_command(
        text
    ):

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

        admins = (
            get_valid_mention_members(
                source,
                group_id,
                get_admins(
                    group_id
                ),
                table_name="admins",
            )
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

        return (
            "멘션 전송에 실패했어요."
        )

    # -----------------------------------------------------
    # 여초 / 남초
    # -----------------------------------------------------

    if text in (
        "*여초",
        "*남초",
        "*여초1",
        "*남초1",
    ):

        members = (
            get_valid_mention_members(
                source,
                group_id,
                get_authenticators(
                    group_id
                ),
                table_name="authenticators",
            )
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

        return (
            "멘션 전송에 실패했어요."
        )

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
            "Health DB 실패",
            exc_info=True,
        )

    return jsonify({
        "status":
            "ok",

        "database":
            (
                "connected"
                if db_ok
                else "disconnected"
            ),

        "gemini":
            bool(
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

    if (
        not LINE_CHANNEL_SECRET
        or not signature
    ):

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
# LINE JSON 파싱
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

        return json.loads(
            decoded
        )

    except Exception:

        logger.warning(
            "LINE JSON 파싱 실패",
            exc_info=True,
        )

        return None


# =========================================================
# LINE Callback
# =========================================================

@app.route(
    "/callback",
    methods=["POST"],
)
def callback():

    # request.get_json() 사용하지 않음
    body = request.get_data(
        cache=False
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

    if not payload:

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

        return

    today = datetime.now(
        KST
    ).date()

    command = is_command(
        normalized_text
    )

    # =====================================================
    # 이름
    # =====================================================

    display_name = get_display_name(
        source,
        group_id,
        user_id,
        force_fetch=False,
    )

    # =====================================================
    # 글자 수
    # =====================================================

    character_count = len(
        re.sub(
            r"\s+",
            "",
            raw_text,
        )
    )

    # =====================================================
    # 마딧수 기록
    # =====================================================

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
            "메시지 기록 실패"
        )

    # =====================================================
    # 일반 채팅은 여기서 종료
    # =====================================================

    if not command:

        schedule_profile_lookup(
            source,
            group_id,
            user_id,
        )

        return

    # =====================================================
    # 명령어
    # =====================================================

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

        # 멘션은 이미 reply_token으로 전송됨
        if (
            reply
            and reply != "__MENTION_SENT__"
        ):

            sent = reply_text(
                reply_token,
                reply,
            )

            if not sent:

                logger.error(
                    "명령어 응답 전송 실패: %r",
                    normalized_text,
                )

    except Exception:

        logger.exception(
            "명령어 실행 실패: %r",
            normalized_text,
        )

        # =================================================
        # 어떤 에러가 나더라도
        # 사용자에게 빈 응답으로 끝내지 않는다.
        # =================================================

        reply_text(
            reply_token,
            (
                "앗, 방금 처리하다가 "
                "잠깐 꼬였어 🧸💦\n"
                "조금 있다가 다시 불러줘!"
            ),
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
                "명령어 처리 지연: %.2fs "
                "command=%r",
                elapsed,
                normalized_text,
            )


# =========================================================
# 시작
# =========================================================

if __name__ == "__main__":

    try:

        init_database()

    except Exception:

        logger.warning(
            "시작 시 DB 초기화 실패",
            exc_info=True,
        )

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "10000",
            )
        ),
    )