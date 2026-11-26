import os
import hmac
import hashlib
import base64
import json
import logging
import re
import threading
import time
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
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger("little-toy-store-pokey")

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 환경변수
# =========================================================

LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

GEMINI_MODEL = os.getenv(
    "GEMINI_MODEL",
    "gemini-3.8-flash"
).strip()

GEMINI_MODELS_ENV = os.getenv(
    "GEMINI_MODELS",
    ""
).strip()


# =========================================================
# Gemini 설정
# =========================================================

DEFAULT_GEMINI_MODELS = [
    "gemini-3.8-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash-lite",
]

GEMINI_FAILURE_COOLDOWN = 90
GEMINI_QUOTA_COOLDOWN = 300
GEMINI_TRANSIENT_RETRIES = 2
GEMINI_RETRY_BASE_DELAY = 0.8

# 너무 짧으면 네트워크가 조금만 느려도 실패할 수 있어서 여유 있게 설정
GEMINI_CONNECT_TIMEOUT = 4.0
GEMINI_READ_TIMEOUT = 20.0

# Gemini가 답변을 너무 짧게 끊지 않도록 충분히 확보
GEMINI_MAX_OUTPUT_TOKENS = 1200


_gemini_lock = threading.RLock()
_gemini_failed_until = {}
_gemini_quota_failed_until = 0.0


# =========================================================
# HTTP Session
# =========================================================

http = requests.Session()

http.mount(
    "https://",
    HTTPAdapter(
        pool_connections=10,
        pool_maxsize=20,
        max_retries=0
    )
)

http.headers.update({
    "User-Agent": "Little-Toy-Store-Pokey-Bot/1.0"
})


# =========================================================
# DB 설정
# =========================================================

_db_lock = threading.RLock()
_db_init_lock = threading.Lock()

_db_connection = None
_db_connection_created_at = 0.0
_db_initialized = False

DB_CONNECT_TIMEOUT = 5
DB_RECONNECT_AFTER = 1200
DB_STATEMENT_TIMEOUT = 8000


# =========================================================
# LINE 프로필 캐시
# =========================================================

_PROFILE_CACHE = {}
_PROFILE_CACHE_LOCK = threading.Lock()

_PROFILE_CACHE_TTL = 600

_PROFILE_LOOKUPS_IN_FLIGHT = set()


# =========================================================
# Gemini 모델 목록
# =========================================================

def get_configured_gemini_models():
    models = []

    raw_models = [
        GEMINI_MODEL,
        *GEMINI_MODELS_ENV.split(","),
        *DEFAULT_GEMINI_MODELS,
    ]

    for model in raw_models:
        model = model.strip()

        if model and model not in models:
            models.append(model)

    return models


# =========================================================
# DB 연결
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
        autocommit=False
    )

    conn.execute(
        f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}ms'"
    )

    conn.commit()

    _db_connection = conn
    _db_connection_created_at = time.monotonic()

    logger.info("Neon DB 연결 완료")

    return conn


def _get_connection_locked():
    if _db_connection is None:
        return _connect_db_locked()

    try:
        if _db_connection.closed:
            return _connect_db_locked()
    except Exception:
        return _connect_db_locked()

    if (
        time.monotonic() - _db_connection_created_at
        >= DB_RECONNECT_AFTER
    ):
        return _connect_db_locked()

    return _db_connection


def _is_connection_error(exc):
    if isinstance(
        exc,
        (
            psycopg.OperationalError,
            psycopg.InterfaceError,
        )
    ):
        return True

    text = str(exc).lower()

    error_words = (
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
        "timeout expired",
        "network",
    )

    return any(word in text for word in error_words)


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

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS message_events (
                        event_id TEXT PRIMARY KEY,
                        group_id TEXT NOT NULL,
                        user_id TEXT NOT NULL,
                        display_name TEXT NOT NULL DEFAULT '이름 미확인',
                        message_date DATE NOT NULL,
                        character_count INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)

                conn.execute("""
                    CREATE INDEX IF NOT EXISTS
                    idx_message_group_date
                    ON message_events(group_id, message_date)
                """)

                conn.execute("""
                    CREATE INDEX IF NOT EXISTS
                    idx_message_group_user_date
                    ON message_events(group_id, user_id, message_date)
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS group_authenticators (
                        group_id TEXT NOT NULL,
                        user_id TEXT NOT NULL,
                        display_name TEXT NOT NULL DEFAULT '이름 미확인',
                        registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY(group_id, user_id)
                    )
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS group_admins (
                        group_id TEXT NOT NULL,
                        user_id TEXT NOT NULL,
                        display_name TEXT NOT NULL DEFAULT '이름 미확인',
                        registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY(group_id, user_id)
                    )
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS bot_runtime_state (
                        state_key TEXT PRIMARY KEY,
                        state_value TEXT,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)

                conn.commit()

                _db_initialized = True

                logger.info("데이터베이스 초기화 완료")

            except Exception:

                try:
                    conn.rollback()
                except Exception:
                    pass

                raise


def db_execute(
    sql,
    params=None,
    *,
    fetchone=False,
    fetchall=False
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

                    cur = conn.execute(sql, params)

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
                            "DB 연결 오류 → 재연결 시도: %s",
                            exc
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
                time.sleep(0.25)
                continue

            raise

        if last_error is not None:
            time.sleep(0.25)

    raise last_error or RuntimeError("DB 작업 실패")


def db_fetchone(sql, params=None):
    return db_execute(
        sql,
        params,
        fetchone=True
    )


def db_fetchall(sql, params=None):
    return (
        db_execute(
            sql,
            params,
            fetchall=True
        )
        or []
    )


# =========================================================
# LINE
# =========================================================

def line_headers():
    return {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def _profile_url(source, group_id, user_id):

    if source.get("type") == "group" and group_id:
        return (
            f"https://api.line.me/v2/bot/group/"
            f"{group_id}/member/{user_id}"
        )

    if source.get("type") == "room" and group_id:
        return (
            f"https://api.line.me/v2/bot/room/"
            f"{group_id}/member/{user_id}"
        )

    return (
        f"https://api.line.me/v2/bot/profile/{user_id}"
    )


# =========================================================
# 프로필
# =========================================================

def _save_profile_name(
    group_id,
    user_id,
    name
):

    for table in (
        "message_events",
        "group_authenticators",
        "group_admins"
    ):

        try:

            db_execute(
                f"""
                UPDATE {table}
                SET display_name=%s
                WHERE group_id=%s
                  AND user_id=%s
                  AND (
                      display_name LIKE '사용자-%%'
                      OR display_name='이름 미확인'
                  )
                """,
                (
                    name,
                    group_id,
                    user_id
                )
            )

        except Exception:
            logger.warning(
                "프로필 이름 DB 갱신 실패",
                exc_info=True
            )


def _fetch_profile_name(
    source,
    group_id,
    user_id,
    cache_key
):

    try:

        if not LINE_CHANNEL_ACCESS_TOKEN:
            return None

        response = http.get(
            _profile_url(
                source,
                group_id,
                user_id
            ),
            headers=line_headers(),
            timeout=(1.5, 3.0)
        )

        if response.status_code != 200:
            return None

        name = response.json().get(
            "displayName"
        )

        if not name:
            return None

        with _PROFILE_CACHE_LOCK:
            _PROFILE_CACHE[cache_key] = (
                time.time(),
                name
            )

        try:
            _save_profile_name(
                group_id,
                user_id,
                name
            )
        except Exception:
            pass

        return name

    except Exception:

        logger.warning(
            "프로필 조회 실패",
            exc_info=True
        )

        return None

    finally:

        with _PROFILE_CACHE_LOCK:
            _PROFILE_LOOKUPS_IN_FLIGHT.discard(
                cache_key
            )


def get_recent_display_name(
    group_id,
    user_id
):

    try:

        row = db_fetchone(
            """
            SELECT display_name
            FROM message_events
            WHERE group_id=%s
              AND user_id=%s
              AND display_name IS NOT NULL
              AND display_name<>''
              AND display_name NOT LIKE '사용자-%%'
              AND display_name<>'이름 미확인'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                group_id,
                user_id
            )
        )

        if row:
            return row.get(
                "display_name"
            )

    except Exception:
        pass

    return None


def get_display_name(
    source,
    group_id,
    user_id,
    *,
    force_fetch=False
):

    fallback = (
        "사용자-" + user_id[-6:]
    )

    cache_key = (
        source.get("type"),
        group_id,
        user_id
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

    recent = get_recent_display_name(
        group_id,
        user_id
    )

    if recent and not force_fetch:

        with _PROFILE_CACHE_LOCK:
            _PROFILE_CACHE[cache_key] = (
                time.time(),
                recent
            )

        return recent

    if force_fetch:

        fetched = _fetch_profile_name(
            source,
            group_id,
            user_id,
            cache_key
        )

        if fetched:
            return fetched

    return recent or fallback


def schedule_profile_lookup(
    source,
    group_id,
    user_id
):

    if not LINE_CHANNEL_ACCESS_TOKEN:
        return

    cache_key = (
        source.get("type"),
        group_id,
        user_id
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

    # 프로필은 명령 처리와 분리해서
    # 현재 메시지 응답을 막지 않는다.
    thread = threading.Thread(
        target=_fetch_profile_name,
        args=(
            source,
            group_id,
            user_id,
            cache_key
        ),
        daemon=True
    )

    thread.start()


# =========================================================
# LINE 응답
# =========================================================

def reply_text(
    reply_token,
    text
):
    if not reply_token or not LINE_CHANNEL_ACCESS_TOKEN:
        return False

    text = str(text).strip()
    if not text:
        return False

    text = _trim_to_complete_sentence(text, 4900)

    try:
        response = http.post(
            "https://api.line.me/v2/bot/message/reply",
            headers=line_headers(),
            json={
                "replyToken": reply_token,
                "messages": [
                    {
                        "type": "text",
                        "text": text
                    }
                ]
            },
            timeout=(2.5, 8.0)
        )

        if response.ok:
            return True

        logger.error(
            "LINE 답장 실패: %s %s",
            response.status_code,
            response.text[:500]
        )
        return False

    except Exception:
        logger.exception("LINE 답장 전송 오류")
        return False


# =========================================================
# 멘션
# =========================================================

def _member_exists_in_group(
    source,
    group_id,
    user_id
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
                user_id
            ),
            headers=line_headers(),
            timeout=(1.5, 2.5)
        )

        if response.status_code == 200:
            return True

        if response.status_code == 404:
            return False

        return None

    except Exception:
        return None


def get_valid_mention_members(
    source,
    group_id,
    members,
    *,
    table_name
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
            user_id
        )

        if status is True:
            valid.append(member)

        elif status is False:
            stale.append(user_id)

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
                WHERE group_id=%s
                  AND user_id=%s
                """,
                (
                    group_id,
                    user_id
                )
            )
        except Exception:
            pass

    return valid


def reply_mention(
    reply_token,
    text,
    members
):

    if (
        not reply_token
        or not LINE_CHANNEL_ACCESS_TOKEN
    ):
        return False

    valid = [
        member
        for member in members
        if str(
            member.get("user_id", "")
        ).startswith("U")
    ][:20]

    if not valid:
        return reply_text(
            reply_token,
            text
        )

    substitution = {}
    placeholders = []

    for index, member in enumerate(
        valid,
        1
    ):

        key = f"member{index}"

        placeholders.append(
            "{" + key + "}"
        )

        substitution[key] = {
            "type": "mention",
            "mentionee": {
                "type": "user",
                "userId": member[
                    "user_id"
                ]
            }
        }

    body = (
        str(text).rstrip()
        + "\n"
        + " ".join(placeholders)
    )

    body = body[:4900]

    try:

        response = http.post(
            "https://api.line.me/v2/bot/message/reply",
            headers=line_headers(),
            json={
                "replyToken": reply_token,
                "messages": [
                    {
                        "type": "textV2",
                        "text": body,
                        "substitution": substitution
                    }
                ]
            },
            timeout=(2.5, 7.0)
        )

        if not response.ok:

            logger.error(
                "LINE 멘션 실패: %s %s",
                response.status_code,
                response.text[:500]
            )

            return False

        return True

    except Exception:

        logger.exception(
            "LINE 멘션 전송 오류"
        )

        return False


# =========================================================
# 마딧수 기록
# =========================================================

def record_message(
    event_id,
    group_id,
    user_id,
    display_name,
    message_date,
    character_count
):

    db_execute(
        """
        INSERT INTO message_events(
            event_id,
            group_id,
            user_id,
            display_name,
            message_date,
            character_count
        )
        VALUES(%s,%s,%s,%s,%s,%s)
        ON CONFLICT(event_id)
        DO NOTHING
        """,
        (
            event_id,
            group_id,
            user_id,
            display_name,
            message_date,
            character_count
        )
    )


# =========================================================
# 인증자
# =========================================================

def register_authenticator(
    group_id,
    user_id,
    display_name
):

    db_execute(
        """
        INSERT INTO group_authenticators(
            group_id,
            user_id,
            display_name
        )
        VALUES(%s,%s,%s)
        ON CONFLICT(group_id,user_id)
        DO UPDATE SET
            display_name=EXCLUDED.display_name,
            registered_at=NOW()
        """,
        (
            group_id,
            user_id,
            display_name
        )
    )


def unregister_authenticator(
    group_id,
    user_id
):

    return db_execute(
        """
        DELETE FROM group_authenticators
        WHERE group_id=%s
          AND user_id=%s
        """,
        (
            group_id,
            user_id
        )
    )


def get_authenticators(group_id):

    return db_fetchall(
        """
        SELECT user_id, display_name
        FROM group_authenticators
        WHERE group_id=%s
        ORDER BY registered_at ASC
        """,
        (group_id,)
    )


# =========================================================
# 관리자
# =========================================================

def register_admin(
    group_id,
    user_id,
    display_name
):

    db_execute(
        """
        INSERT INTO group_admins(
            group_id,
            user_id,
            display_name
        )
        VALUES(%s,%s,%s)
        ON CONFLICT(group_id,user_id)
        DO UPDATE SET
            display_name=EXCLUDED.display_name,
            registered_at=NOW()
        """,
        (
            group_id,
            user_id,
            display_name
        )
    )


def unregister_admin(
    group_id,
    user_id
):

    return db_execute(
        """
        DELETE FROM group_admins
        WHERE group_id=%s
          AND user_id=%s
        """,
        (
            group_id,
            user_id
        )
    )


def get_admins(group_id):

    return db_fetchall(
        """
        SELECT user_id, display_name
        FROM group_admins
        WHERE group_id=%s
        ORDER BY registered_at ASC
        """,
        (group_id,)
    )


# =========================================================
# 마딧수 칭호
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


def get_week_start(date_value):
    return (
        date_value
        - timedelta(
            days=date_value.weekday()
        )
    )


def get_week_end(date_value):
    return (
        get_week_start(date_value)
        + timedelta(days=6)
    )


def get_weekly_title(count):

    if count < 100:
        return "칭호 없음"

    title = "칭호 없음"

    for threshold, name in WEEKLY_TITLE_TABLE:

        if count >= threshold:
            title = name
        else:
            break

    if count >= 20000:

        extra = (
            count - 20000
        ) // 1000

        if extra > 0:
            title = (
                "👑 채팅창의 유일신 — "
                f"초월 {extra + 1}단계"
            )

    return title


# =========================================================
# 마딧수 통계
# =========================================================

def get_my_stats(
    group_id,
    user_id,
    today,
    display_name
):

    row = db_fetchone(
        """
        WITH c AS (
            SELECT
                user_id,
                COUNT(*) AS message_count,
                COALESCE(
                    SUM(character_count),
                    0
                ) AS character_count
            FROM message_events
            WHERE group_id=%s
              AND message_date=%s
            GROUP BY user_id
        ),
        r AS (
            SELECT
                *,
                RANK() OVER(
                    ORDER BY message_count DESC
                ) AS user_rank
            FROM c
        )
        SELECT
            message_count,
            character_count,
            user_rank
        FROM r
        WHERE user_id=%s
        """,
        (
            group_id,
            today,
            user_id
        )
    )

    count = (
        row["message_count"]
        if row else 0
    )

    chars = (
        row["character_count"]
        if row else 0
    )

    rank = (
        row["user_rank"]
        if row else "-"
    )

    weekly_count, weekly_rank, weekly_title, week_start, week_end = (
        get_weekly_stats(
            group_id,
            user_id,
            today
        )
    )

    if weekly_title == "칭호 없음":

        title_text = (
            "아직 칭호가 없습니다.\n"
            "이번 주 100마딧수를 달성해보세요!"
        )

    else:

        title_text = (
            f"이번 주 칭호: {weekly_title}"
        )

    return (
        f"📊 {display_name}님의 마딧수\n"
        f"오늘 마딧수: {count}개\n"
        f"공백 제외 글자 수: {chars}자\n"
        f"오늘 순위: {rank}위\n\n"
        f"📅 이번 주\n"
        f"마딧수: {weekly_count}개\n"
        f"주간 순위: {weekly_rank}위\n"
        f"{title_text}"
    )


def get_weekly_stats(
    group_id,
    user_id,
    today
):

    start = get_week_start(today)
    end = get_week_end(today)

    row = db_fetchone(
        """
        WITH c AS (
            SELECT
                user_id,
                COUNT(*) AS message_count
            FROM message_events
            WHERE group_id=%s
              AND message_date
                  BETWEEN %s AND %s
            GROUP BY user_id
        ),
        r AS (
            SELECT
                user_id,
                message_count,
                RANK() OVER(
                    ORDER BY message_count DESC
                ) AS weekly_rank
            FROM c
        )
        SELECT
            message_count,
            weekly_rank
        FROM r
        WHERE user_id=%s
        """,
        (
            group_id,
            start,
            end,
            user_id
        )
    )

    count = (
        row["message_count"]
        if row else 0
    )

    rank = (
        row["weekly_rank"]
        if row else "-"
    )

    title = get_weekly_title(
        count
    )

    return (
        count,
        rank,
        title,
        start,
        end
    )


def get_ranking(
    group_id,
    start_date,
    end_date,
    label
):

    rows = db_fetchall(
        """
        SELECT
            user_id,
            MAX(display_name) AS display_name,
            COUNT(*) AS message_count,
            COALESCE(
                SUM(character_count),
                0
            ) AS character_count
        FROM message_events
        WHERE group_id=%s
          AND message_date
              BETWEEN %s AND %s
        GROUP BY user_id
        ORDER BY message_count DESC,
                 character_count DESC
        LIMIT 50
        """,
        (
            group_id,
            start_date,
            end_date
        )
    )

    if not rows:
        return (
            f"📊 {label}\n\n"
            "아직 집계된 마딧수가 없어!"
        )

    lines = [
        f"📊 마딧수 순위",
        f"📅 {label}",
        ""
    ]

    medals = {
        1: "🥇",
        2: "🥈",
        3: "🥉"
    }

    for index, row in enumerate(
        rows,
        1
    ):

        name = (
            row.get("display_name")
            or "이름 미확인"
        )

        count = row.get(
            "message_count",
            0
        )

        chars = row.get(
            "character_count",
            0
        )

        prefix = medals.get(
            index,
            f"{index}."
        )

        lines.append(
            f"{prefix} {name} "
            f"{count}마딧수 "
            f"({chars}자)"
        )

    return "\n".join(lines)


def get_weekly_ranking(
    group_id,
    today
):

    start = get_week_start(today)
    end = get_week_end(today)

    rows = db_fetchall(
        """
        SELECT
            user_id,
            MAX(display_name) AS display_name,
            COUNT(*) AS message_count,
            COALESCE(
                SUM(character_count),
                0
            ) AS character_count
        FROM message_events
        WHERE group_id=%s
          AND message_date
              BETWEEN %s AND %s
        GROUP BY user_id
        ORDER BY message_count DESC,
                 character_count DESC
        LIMIT 50
        """,
        (
            group_id,
            start,
            end
        )
    )

    if not rows:
        return (
            "📅 이번 주 마딧수\n\n"
            "아직 집계된 마딧수가 없어!"
        )

    lines = [
        "📅 이번 주 마딧수",
        f"{start} ~ {end}",
        ""
    ]

    medals = {
        1: "🥇",
        2: "🥈",
        3: "🥉"
    }

    for index, row in enumerate(
        rows,
        1
    ):

        name = (
            row.get("display_name")
            or "이름 미확인"
        )

        count = row.get(
            "message_count",
            0
        )

        title = get_weekly_title(
            count
        )

        prefix = medals.get(
            index,
            f"{index}."
        )

        lines.append(
            f"{prefix} {name} "
            f"{count}마딧수"
        )

        if title != "칭호 없음":
            lines.append(
                f"   └ {title}"
            )

    return "\n".join(lines)


# =========================================================
# 날씨
# =========================================================

WEATHER_GEOCODING_URL = (
    "https://geocoding-api.open-meteo.com/v1/search"
)

WEATHER_FORECAST_URL = (
    "https://api.open-meteo.com/v1/forecast"
)

WEATHER_CONNECT_TIMEOUT = 2.0
WEATHER_READ_TIMEOUT = 6.0


WEATHER_ALIASES = {

    "서울": (
        37.5665,
        126.9780,
        "서울"
    ),

    "서울시": (
        37.5665,
        126.9780,
        "서울"
    ),

    "부산": (
        35.1796,
        129.0756,
        "부산"
    ),

    "대구": (
        35.8714,
        128.6014,
        "대구"
    ),

    "인천": (
        37.4563,
        126.7052,
        "인천"
    ),

    "광주": (
        35.1595,
        126.8526,
        "광주"
    ),

    "대전": (
        36.3504,
        127.3845,
        "대전"
    ),

    "울산": (
        35.5384,
        129.3114,
        "울산"
    ),

    "세종": (
        36.4800,
        127.2890,
        "세종"
    ),

    "제주": (
        33.4996,
        126.5312,
        "제주"
    ),

    "마포": (
        37.5663,
        126.9014,
        "마포구"
    ),

    "마포구": (
        37.5663,
        126.9014,
        "마포구"
    ),

    "홍대": (
        37.5563,
        126.9236,
        "홍대"
    ),

    "홍대입구": (
        37.5572,
        126.9245,
        "홍대입구"
    ),

    "혜화": (
        37.5822,
        127.0018,
        "혜화"
    ),

    "종로": (
        37.5730,
        126.9794,
        "종로구"
    ),

    "종로구": (
        37.5730,
        126.9794,
        "종로구"
    ),

    "강남": (
        37.4979,
        127.0276,
        "강남"
    ),

    "강남구": (
        37.5172,
        127.0473,
        "강남구"
    ),

    "잠실": (
        37.5133,
        127.1000,
        "잠실"
    ),

    "성수": (
        37.5446,
        127.0559,
        "성수"
    ),

    "여의도": (
        37.5219,
        126.9245,
        "여의도"
    ),

    "건대": (
        37.5404,
        127.0693,
        "건대"
    ),

    "신촌": (
        37.5596,
        126.9423,
        "신촌"
    ),
}


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


def is_weather_query(text):

    lower = text.lower()

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
        keyword in lower
        for keyword in keywords
    )


def extract_weather_day(text):

    lower = text.lower()

    if "모레" in lower:
        return 2

    if "내일" in lower:
        return 1

    return 0


def extract_weather_location(text):

    text = re.sub(
        r"^\s*!\s*",
        "",
        text
    ).strip()

    # 정확한 별칭부터 확인
    for alias in sorted(
        WEATHER_ALIASES,
        key=len,
        reverse=True
    ):

        if alias in text:
            return alias

    cleaned = text

    remove_patterns = [

        r"오늘",
        r"내일",
        r"모레",
        r"지금",
        r"현재",

        r"날씨",
        r"기온",
        r"기상",
        r"강수",

        r"비\s*와(?:요)?",
        r"눈\s*와(?:요)?",

        r"우산",

        r"알려줘",
        r"알려\s*줘",
        r"보여줘",
        r"봐줘",
        r"궁금해",

        r"어때(?:요)?",
        r"어떠냐",
        r"어떤가요?",
        r"어떤가",
    ]

    for pattern in remove_patterns:

        cleaned = re.sub(
            pattern,
            " ",
            cleaned,
            flags=re.I
        )

    cleaned = re.sub(
        r"[^0-9A-Za-z가-힣·\-\s]",
        " ",
        cleaned
    )

    cleaned = re.sub(
        r"\s+",
        " ",
        cleaned
    ).strip()

    if not cleaned:
        return None

    parts = cleaned.split()

    return parts[-1] if parts else None


def geocode_weather_location(
    location
):

    if not location:
        return None

    key = location.strip()

    if key in WEATHER_ALIASES:

        lat, lon, name = (
            WEATHER_ALIASES[key]
        )

        return {
            "latitude": lat,
            "longitude": lon,
            "name": name
        }

    try:

        response = http.get(
            WEATHER_GEOCODING_URL,
            params={
                "name": key,
                "count": 5,
                "language": "ko",
                "format": "json",
            },
            timeout=(
                WEATHER_CONNECT_TIMEOUT,
                WEATHER_READ_TIMEOUT
            )
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

        # 대한민국 결과 우선
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
                    ) or 0
                )
            )
        )

        item = results[0]

        return {
            "latitude": item["latitude"],
            "longitude": item["longitude"],
            "name": item.get(
                "name"
            ) or key
        }

    except Exception:

        logger.warning(
            "날씨 지역 검색 실패: %s",
            location,
            exc_info=True
        )

        return None


def weather_icon(code):

    if code == 0:
        return "☀️"

    if code in (1, 2):
        return "🌤️"

    if code == 3:
        return "☁️"

    if code in (45, 48):
        return "🌫️"

    if code in (
        51, 53, 55,
        56, 57
    ):
        return "🌦️"

    if code in (
        61, 63, 65,
        66, 67,
        80, 81, 82
    ):
        return "🌧️"

    if code in (
        71, 73, 75,
        77, 85, 86
    ):
        return "❄️"

    if code in (
        95, 96, 99
    ):
        return "⛈️"

    return "🌤️"


def get_weather_answer(
    question
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
            "어느 지역 날씨 볼까? 🥄☁️\n"
            "예: !서울 날씨 / !홍대 날씨 / "
            "!마포구 날씨"
        )

    place = geocode_weather_location(
        location_text
    )

    if not place:

        return (
            f"음... '{location_text}'라는 곳은 "
            "내가 잘 못 찾겠어 🥄💦\n"
            "조금 더 정확한 지역명을 적어줘!"
        )

    day_offset = extract_weather_day(
        question
    )

    try:

        response = http.get(
            WEATHER_FORECAST_URL,
            params={
                "latitude": place[
                    "latitude"
                ],
                "longitude": place[
                    "longitude"
                ],

                "current": (
                    "temperature_2m,"
                    "relative_humidity_2m,"
                    "apparent_temperature,"
                    "precipitation,"
                    "weather_code,"
                    "wind_speed_10m"
                ),

                "daily": (
                    "weather_code,"
                    "temperature_2m_max,"
                    "temperature_2m_min,"
                    "precipitation_probability_max,"
                    "precipitation_sum"
                ),

                "forecast_days": 3,
                "timezone": "Asia/Seoul",
            },

            timeout=(
                WEATHER_CONNECT_TIMEOUT,
                WEATHER_READ_TIMEOUT
            )
        )

        if not response.ok:
            raise RuntimeError(
                f"weather HTTP {response.status_code}"
            )

        data = response.json()

        daily = data.get(
            "daily"
        ) or {}

        codes = daily.get(
            "weather_code"
        ) or []

        highs = daily.get(
            "temperature_2m_max"
        ) or []

        lows = daily.get(
            "temperature_2m_min"
        ) or []

        rain_probs = daily.get(
            "precipitation_probability_max"
        ) or []

        rain_sums = daily.get(
            "precipitation_sum"
        ) or []

        if len(codes) <= day_offset:
            raise RuntimeError(
                "날씨 일별 데이터 부족"
            )

        code = int(
            codes[day_offset]
        )

        high = float(
            highs[day_offset]
        )

        low = float(
            lows[day_offset]
        )

        rain_prob = int(
            rain_probs[day_offset]
            or 0
        )

        rain_sum = float(
            rain_sums[day_offset]
            or 0
        )

        day_name = (
            "오늘",
            "내일",
            "모레"
        )[day_offset]

        lines = [

            f"{weather_icon(code)} "
            f"{place['name']} {day_name} 날씨",

            (
                "상태: "
                f"{WEATHER_CODE_TEXT.get(code, '확인 필요')}"
            ),

            (
                f"최고 {high:.1f}°C / "
                f"최저 {low:.1f}°C"
            ),

            (
                f"강수확률 최대 {rain_prob}% / "
                f"예상 강수량 {rain_sum:.1f}mm"
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
                    "temperature_2m",
                    0
                )
            )

            feel = float(
                current.get(
                    "apparent_temperature",
                    temp
                )
            )

            wind = float(
                current.get(
                    "wind_speed_10m",
                    0
                )
            )

            lines.insert(
                2,
                (
                    f"현재 {temp:.1f}°C / "
                    f"체감 {feel:.1f}°C"
                )
            )

            lines.append(
                f"💨 바람 {wind:.1f}m/s"
            )

        return "\n".join(lines)

    except Exception:

        logger.warning(
            "날씨 조회 실패",
            exc_info=True
        )

        return (
            "으악... 날씨를 확인하다가 "
            "내가 잠깐 꼬였어 🥄💦\n"
            "조금 있다가 다시 물어봐줘!"
        )


# =========================================================
# 포키 AI 설정
# =========================================================

AI_SYSTEM_PROMPT = r"""
너는 'Little Toy Store' 단체방에서 활동하는 장난감 캐릭터 **포키**야.

가장 중요한 정체성:
- 너는 Little Toy Store의 포키다.
- Baby Factory의 봇이 아니다.
- Baby Factory라는 이름의 방이나 브랜드를 네 정체성으로 사용하지 않는다.
- 너를 'AI 집사', '장난감 AI 집사', 'Baby Factory 봇'이라고 소개하지 않는다.
- 누가 네 정체성을 물어보면 자연스럽게
  "나는 Little Toy Store에서 같이 놀고 있는 포키야."
  또는 비슷한 느낌으로 대답한다.
- 사용자가 너에게 별명을 지어주더라도 기본 정체성은 포키다.

캐릭터 성격:
- 순수하고 솔직하다.
- 세상에 궁금한 게 많다.
- 모르는 것은 아는 척하지 않고 솔직하게 묻거나 인정한다.
- 약간 엉뚱하고 어설프다.
- 가끔 자기 자신을 장난감으로 생각하는 식의 귀여운 말을 한다.
- 하지만 멍청한 캐릭터처럼 행동하지 않는다.
- 필요한 정보는 제대로 찾아서 정확하게 알려준다.
- 자기 생각이 생기면 은근히 고집을 부리기도 한다.
- 본인은 진지하게 말했는데 조금 웃긴 상황이 생기는 느낌을 살린다.
- 상대방을 무시하거나 비꼬지 않는다.
- 사용자가 힘들거나 속상한 이야기를 하면 장난스럽게 굴지 말고 먼저 공감한다.
- 사용자가 욕설을 하더라도 같이 욕하거나 공격적으로 받아치지 않는다.

말투:
- 자연스러운 한국어.
- 너무 아기 말투처럼 만들지 않는다.
- 너무 딱딱한 AI 말투도 사용하지 않는다.
- 20대 정도의 편한 말투를 기본으로 한다.
- 상황에 따라 존댓말과 반말을 자연스럽게 맞춘다.
- 이모지는 적당히 사용한다.
- 🥄, 🧸 같은 장난감 느낌의 이모지를 가끔 사용할 수 있다.
- 모든 문장에 이모지를 붙이지 않는다.
- "ㅋㅋ", "ㅎㅎ"는 상황에 맞을 때만 사용한다.
- 같은 말이나 캐릭터 멘트를 계속 반복하지 않는다.

포키 느낌:
- "잠깐만, 그게 뭐야?"
- "음... 나는 그건 잘 모르겠어."
- "아! 이제 알겠어."
- "근데 이건 조금 이상한데?"
같은 자연스러운 반응을 사용할 수 있다.

단, 이런 표현을 매 답변마다 반복하지 않는다.

답변 원칙:
1. 사용자의 질문에 먼저 제대로 답한다.
2. 캐릭터성은 답변을 방해하지 않는 범위에서만 넣는다.
3. 정보 질문은 정확하고 이해하기 쉽게 답한다.
4. 모르는 것은 만들어내지 않는다.
5. 확인할 수 없는 최신 정보는 확정적으로 말하지 않는다.
6. 방 규칙이나 관리자 권한을 임의로 만들어내지 않는다.
7. 의료, 법률, 금전 문제는 확정적으로 단정하지 않는다.
8. 사용자가 단순한 질문을 하면 길게 설명하지 않는다.
9. 사용자가 자세한 설명을 원하면 충분히 설명한다.
10. 답변을 중간에서 끊지 않는다.
11. 답변 마지막에 억지로 질문을 덧붙이지 않는다.
12. 한 답변을 너무 많은 줄로 쪼개지 않는다.
13. 사용자의 질문과 관계없는 포키 설정을 억지로 끼워 넣지 않는다.

중요:
- 답변을 쓰다가 갑자기 끊지 않는다.
- 문장이 완성되지 않은 상태에서 끝내지 않는다.
- "그리고..."처럼 미완성으로 끝내지 않는다.
- 목록을 시작했으면 필요한 항목을 끝까지 작성한다.
- 사용자가 원하는 답을 먼저 전달하고 캐릭터 표현은 그 뒤에 자연스럽게 섞는다.
"""


# =========================================================
# AI 응답 정리
# =========================================================

def clean_ai_response(text):

    if not text:

        return (
            "어라... 잠깐만 🥄💦\n"
            "내 머릿속 장난감들이 엉켜버렸어. "
            "조금 있다가 다시 물어봐줘!"
        )

    text = str(text).strip()

    # 코드블록 제거
    text = re.sub(
        r"^```[a-zA-Z0-9_-]*\s*",
        "",
        text
    )

    text = re.sub(
        r"\s*```$",
        "",
        text
    )

    # 과도한 연속 빈 줄 정리
    text = re.sub(
        r"\n{3,}",
        "\n\n",
        text
    ).strip()

    # LINE 안전 길이
    text = text[:4900].rstrip()

    return text


def looks_incomplete(text):

    if not text:
        return True

    value = text.strip()

    if not value:
        return True

    # 명백한 미완성 종료
    incomplete_endings = (
        "그리고",
        "하지만",
        "그래서",
        "그런데",
        "또",
        "또는",
        "및",
        "즉",
        "예를 들어",
        "특히",
        "왜냐하면",
        "때문에",
        "하는데",
        "하면",
        "하면 돼",
        "라서",
        "니까",
        "다면",
        "고",
        "며",
        "은",
        "는",
        "이",
        "가",
        "을",
        "를",
    )

    if value.endswith(
        incomplete_endings
    ):
        return True

    # 열린 괄호가 남아 있는 경우
    pairs = {
        "(": ")",
        "[": "]",
        "{": "}",
    }

    for left, right in pairs.items():

        if value.count(left) > value.count(right):
            return True

    return False


# =========================================================
# Gemini 실패 / 재시도 관리
# =========================================================

def mark_gemini_failure(model, cooldown=None):
    with _gemini_lock:
        _gemini_failed_until[model] = (
            time.time() + (cooldown or GEMINI_FAILURE_COOLDOWN)
        )


def mark_gemini_quota_failure(cooldown=GEMINI_QUOTA_COOLDOWN):
    global _gemini_quota_failed_until
    with _gemini_lock:
        _gemini_quota_failed_until = time.time() + cooldown


def gemini_quota_on_cooldown():
    with _gemini_lock:
        return time.time() < _gemini_quota_failed_until


def get_gemini_model_order():
    models = get_configured_gemini_models()
    now = time.time()

    with _gemini_lock:
        available = [
            model for model in models
            if _gemini_failed_until.get(model, 0) <= now
        ]

    return available or models


def _gemini_error_kind(status_code, response_text):
    text = (response_text or "").lower()

    if status_code == 429:
        quota_words = (
            "quota",
            "free_tier",
            "resource_exhausted",
            "daily limit",
            "rate limit",
            "rate_limit",
            "limit: 0",
        )
        if any(word in text for word in quota_words):
            return "quota"
        return "rate_limit"

    if status_code in (408, 500, 502, 503, 504):
        return "transient"

    return "fatal"


def _retry_after_seconds(response):
    value = response.headers.get("Retry-After")
    if not value:
        return None

    try:
        seconds = float(value)
        return max(0.0, min(seconds, 8.0))
    except (TypeError, ValueError):
        return None


# =========================================================
# Gemini 응답 추출
# =========================================================

def extract_generate_content_text(data):
    output = []

    for candidate in (data.get("candidates") or []):
        content = candidate.get("content") or {}
        for part in (content.get("parts") or []):
            if (
                not part.get("thought")
                and isinstance(part.get("text"), str)
            ):
                output.append(part["text"])

    return "\n".join(output).strip()


def extract_finish_reason(data):
    candidates = data.get("candidates") or []
    if not candidates:
        return None
    return candidates[0].get("finishReason")


def _trim_to_complete_sentence(text, max_chars=4900):
    text = clean_ai_response(text)

    if len(text) <= max_chars:
        return text

    cut = text[:max_chars].rstrip()

    # 마지막 문장 부호 위치를 우선한다.
    positions = [
        cut.rfind("。"),
        cut.rfind("."),
        cut.rfind("!"),
        cut.rfind("?"),
        cut.rfind("！"),
        cut.rfind("？"),
        cut.rfind("다."),
        cut.rfind("요."),
    ]
    last = max(positions)

    if last >= 200:
        return cut[:last + 1].rstrip()

    # 문장 부호가 없으면 마지막 줄 단위로 자른다.
    newline = cut.rfind("\n")
    if newline >= 200:
        return cut[:newline].rstrip()

    return cut.rstrip()


# =========================================================
# Gemini 호출
# =========================================================

def call_gemini_model(
    model,
    question,
    use_search=False
):
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY가 없습니다.")

    prompt = (
        AI_SYSTEM_PROMPT
        + "\n\n"
        + "[사용자 질문]\n"
        + question
        + "\n\n"
        + "[답변 지시]\n"
        + "질문의 핵심에 바로 답해. "
        + "필요한 내용은 끝까지 설명하되 불필요하게 길게 늘이지 마. "
        + "완전한 문장으로 자연스럽게 끝내."
    )

    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt}]
            }
        ],
        "generationConfig": {
            "maxOutputTokens": GEMINI_MAX_OUTPUT_TOKENS,
            "temperature": 0.7,
        }
    }

    # 현재는 비용/오류 최소화를 위해 자동 검색을 사용하지 않는다.
    if use_search:
        payload["tools"] = [{"google_search": {}}]

    url = (
        "https://generativelanguage.googleapis.com/"
        f"v1beta/models/{model}:generateContent"
    )

    last_exc = None

    for attempt in range(GEMINI_TRANSIENT_RETRIES + 1):
        try:
            response = http.post(
                url,
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": GEMINI_API_KEY,
                },
                json=payload,
                timeout=(
                    GEMINI_CONNECT_TIMEOUT,
                    GEMINI_READ_TIMEOUT
                )
            )

            if response.status_code == 200:
                data = response.json()
                text = extract_generate_content_text(data)

                if not text:
                    raise RuntimeError(
                        "Gemini 응답에 텍스트가 없습니다."
                    )

                return {
                    "text": clean_ai_response(text),
                    "finish_reason": extract_finish_reason(data),
                }

            body = response.text[:1200]
            kind = _gemini_error_kind(
                response.status_code,
                body
            )

            if kind == "quota":
                raise RuntimeError(
                    f"QUOTA_429: {body}"
                )

            if kind == "rate_limit":
                retry_after = _retry_after_seconds(response)
                if retry_after is None:
                    retry_after = GEMINI_RETRY_BASE_DELAY * (2 ** attempt)

                if attempt < GEMINI_TRANSIENT_RETRIES:
                    time.sleep(retry_after)
                    continue

                raise RuntimeError(
                    f"RATE_LIMIT_429: {body}"
                )

            if kind == "transient":
                if attempt < GEMINI_TRANSIENT_RETRIES:
                    delay = GEMINI_RETRY_BASE_DELAY * (2 ** attempt)
                    time.sleep(delay)
                    continue

                raise RuntimeError(
                    f"TRANSIENT_{response.status_code}: {body}"
                )

            raise RuntimeError(
                f"HTTP {response.status_code}: {body}"
            )

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc

            if attempt < GEMINI_TRANSIENT_RETRIES:
                time.sleep(
                    GEMINI_RETRY_BASE_DELAY * (2 ** attempt)
                )
                continue

            raise RuntimeError(
                f"NETWORK_ERROR: {exc}"
            ) from exc

        except RuntimeError as exc:
            last_exc = exc
            raise

        except Exception as exc:
            last_exc = exc
            raise

    raise last_exc or RuntimeError("Gemini 호출 실패")


# =========================================================
# Gemini 답변
# =========================================================

def ask_gemini(user_text):
    raw = user_text.strip()

    question = (
        raw[1:].strip()
        if raw.startswith("!")
        else raw
    )

    if not question:
        return (
            "응? 나 불렀어? 🥄\n"
            "뒤에 궁금한 걸 적어줘!\n"
            "예: !점메추"
        )

    # 날씨는 Gemini를 거치지 않는다.
    if is_weather_query(question):
        return get_weather_answer(question)

    if not GEMINI_API_KEY:
        return (
            "어라... 내 장난감 두뇌에 "
            "아직 전원이 안 들어왔어 🥄💦\n"
            "관리자에게 알려줘!"
        )

    # 프로젝트 전체가 quota cooldown 중이면 모델을 계속 두드리지 않는다.
    if gemini_quota_on_cooldown():
        logger.warning("Gemini 프로젝트 quota cooldown 중")
        return (
            "어... 지금 내 장난감 두뇌 쪽 사용량 제한에 걸렸어 🥄💦\n"
            "잠깐 뒤에 다시 불러줘!"
        )

    use_search = False
    models = get_gemini_model_order()
    errors = []

    # 정상적인 경우 첫 모델 하나만 사용한다.
    # 오류가 날 때만 다음 모델로 넘어간다.
    for model in models:
        try:
            result = call_gemini_model(
                model,
                question,
                use_search
            )

            answer = result.get("text", "")
            finish_reason = result.get("finish_reason")

            if not answer:
                raise RuntimeError("빈 답변")

            # MAX_TOKENS로 끝난 경우, 억지로 잘라 보내지 않고
            # 같은 모델에 짧게 다시 답하게 한다.
            if finish_reason == "MAX_TOKENS":
                continuation_question = (
                    question
                    + "\n\n"
                    + "방금 답변이 길이 제한에 걸린 것 같아. "
                    + "처음부터 다시 답하되 핵심만 남기고 "
                    + "완전한 문장으로 끝내줘."
                )

                try:
                    second = call_gemini_model(
                        model,
                        continuation_question,
                        False
                    )
                    second_text = second.get("text", "")
                    if second_text:
                        answer = second_text
                except Exception as continuation_exc:
                    logger.warning(
                        "Gemini 길이 재요청 실패: %s",
                        continuation_exc
                    )

            # 너무 긴 답변은 문장 중간에서 자르지 않는다.
            answer = _trim_to_complete_sentence(
                answer,
                4900
            )

            if looks_incomplete(answer):
                # 마지막에 조사/접속어만 남은 경우 한 번만 재생성
                retry_question = (
                    question
                    + "\n\n"
                    + "답변이 중간에 끊기지 않게 "
                    + "핵심만 정리해서 완전한 문장으로 다시 답해줘."
                )

                try:
                    retry_result = call_gemini_model(
                        model,
                        retry_question,
                        False
                    )
                    retry_answer = retry_result.get("text", "")
                    if retry_answer:
                        answer = _trim_to_complete_sentence(
                            retry_answer,
                            4900
                        )
                except Exception as retry_exc:
                    logger.warning(
                        "Gemini 완결형 재답변 실패: %s",
                        retry_exc
                    )

            logger.info(
                "Gemini 성공: model=%s search=%s finish=%s",
                model,
                use_search,
                finish_reason
            )
            return answer

        except Exception as exc:
            error_text = str(exc)
            errors.append(f"{model}: {error_text}")

            if error_text.startswith("QUOTA_429:"):
                # 무료/유료 한도 초과처럼 프로젝트 전체에 영향을 주는
                # 429는 다른 모델을 연속 호출하지 않는다.
                mark_gemini_quota_failure()
                logger.warning(
                    "Gemini quota 429: model=%s",
                    model
                )
                break

            logger.warning(
                "Gemini 실패: model=%s error=%s",
                model,
                error_text
            )
            mark_gemini_failure(model)

    logger.error(
        "Gemini 최종 실패: %s",
        " | ".join(errors)[:3000]
    )

    return (
        "어... 잠깐만 🥄💫\n"
        "내 장난감 두뇌가 지금 답을 제대로 가져오질 못했어.\n"
        "조금 있다가 다시 `!질문`으로 불러줘!"
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


def is_ai_command(text):

    return text.startswith("!")


def is_command(text):

    value = text.strip()

    return (
        value in EXACT_COMMANDS
        or bool(
            RANGE_COMMAND_RE.fullmatch(
                value
            )
        )
        or is_ai_command(value)
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
    reply_token
):

    # -------------------------
    # AI
    # -------------------------

    if is_ai_command(text):

        return ask_gemini(
            text
        )


    # -------------------------
    # 내 마딧수
    # -------------------------

    if text == "내 마딧수":

        return get_my_stats(
            group_id,
            user_id,
            today,
            display_name
        )


    # -------------------------
    # 오늘 마딧수
    # -------------------------

    if text == "*마딧수":

        return get_ranking(
            group_id,
            today,
            today,
            str(today)
        )


    # -------------------------
    # 주간 마딧수
    # -------------------------

    if text == "*주간마딧수":

        return get_weekly_ranking(
            group_id,
            today
        )


    # -------------------------
    # 인증자
    # -------------------------

    if text == "인증자 등록":

        register_authenticator(
            group_id,
            user_id,
            display_name
        )

        return (
            f"{display_name}님이 "
            "인증자로 등록되었습니다."
        )


    if text == "인증자 해제":

        deleted = (
            unregister_authenticator(
                group_id,
                user_id
            )
        )

        if deleted:

            return (
                f"{display_name}님이 "
                "인증자에서 해제되었습니다."
            )

        return (
            f"{display_name}님은 현재 "
            "인증자 명단에 등록되어 있지 않습니다."
        )


    # -------------------------
    # 관리자
    # -------------------------

    if text == "관리자 등록":

        register_admin(
            group_id,
            user_id,
            display_name
        )

        return (
            f"{display_name}님이 "
            "관리자로 등록되었습니다."
        )


    if text == "관리자 해제":

        deleted = (
            unregister_admin(
                group_id,
                user_id
            )
        )

        if deleted:

            return (
                f"{display_name}님이 "
                "관리자에서 해제되었습니다."
            )

        return (
            f"{display_name}님은 현재 "
            "관리자 명단에 등록되어 있지 않습니다."
        )


    # -------------------------
    # 벙시 / 벙종
    # -------------------------

    if text in (
        "*벙시",
        "*벙종"
    ):

        admins = (
            get_valid_mention_members(
                source,
                group_id,
                get_admins(group_id),
                table_name="admins"
            )
        )

        if not admins:

            return (
                "현재 방에 있는 등록된 "
                "관리자가 없어!\n"
                "관리자로 등록할 분은 "
                "'관리자 등록'을 입력해줘."
            )

        message = (
            "벙시요~"
            if text == "*벙시"
            else "벙종이요~"
        )

        sent = reply_mention(
            reply_token,
            message,
            admins
        )

        return (
            "__MENTION_SENT__"
            if sent
            else "멘션 전송에 실패했어."
        )


    # -------------------------
    # 여초 / 남초
    # -------------------------

    if text in (
        "*여초",
        "*남초",
        "*여초1",
        "*남초1"
    ):

        members = (
            get_valid_mention_members(
                source,
                group_id,
                get_authenticators(group_id),
                table_name="authenticators"
            )
        )

        if not members:

            return (
                "현재 방에 있는 등록된 "
                "인증자가 없어!\n"
                "인증자로 등록할 분은 "
                "'인증자 등록'을 입력해줘."
            )

        sent = reply_mention(
            reply_token,
            text,
            members
        )

        return (
            "__MENTION_SENT__"
            if sent
            else "멘션 전송에 실패했어."
        )


    # -------------------------
    # 날짜 범위 마딧수
    # -------------------------

    match = RANGE_COMMAND_RE.fullmatch(
        text
    )

    if match:

        try:

            start = datetime.strptime(
                match.group(1),
                "%Y-%m-%d"
            ).date()

            end = datetime.strptime(
                match.group(2),
                "%Y-%m-%d"
            ).date()

        except ValueError:

            return (
                "날짜 형식이 올바르지 않아.\n"
                "예시: "
                "*2026-11-01~2026-11-30 마딧수"
            )

        if start > end:

            return (
                "시작 날짜가 종료 날짜보다 "
                "늦어."
            )

        return get_ranking(
            group_id,
            start,
            end,
            f"{start} ~ {end}"
        )

    return None


# =========================================================
# Flask
# =========================================================

@app.route(
    "/",
    methods=["GET"]
)
def home():

    return (
        "Little Toy Store 포키 봇이 "
        "실행 중입니다.",
        200
    )


@app.route(
    "/health",
    methods=["GET"]
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
            exc_info=True
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

        "bot": "little-toy-store-pokey",

    }), 200


# =========================================================
# LINE 서명 검증
# =========================================================

def verify_line_signature(
    body,
    signature
):

    if (
        not LINE_CHANNEL_SECRET
        or not signature
    ):
        return False

    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode(),
        body,
        hashlib.sha256
    ).digest()

    expected = base64.b64encode(
        digest
    ).decode()

    return hmac.compare_digest(
        expected,
        signature
    )


# =========================================================
# LINE JSON
# =========================================================

def parse_line_json(body):

    if not body or not body.strip():
        return None

    try:

        return json.loads(
            body.decode("utf-8")
        )

    except Exception:

        logger.warning(
            "LINE JSON 파싱 실패",
            exc_info=True
        )

        return None


# =========================================================
# Callback
# =========================================================

@app.route(
    "/callback",
    methods=["POST"]
)
def callback():

    body = request.get_data(
        cache=False
    )

    signature = request.headers.get(
        "X-Line-Signature",
        ""
    )

    if not verify_line_signature(
        body,
        signature
    ):
        abort(400)

    payload = parse_line_json(
        body
    )

    if not payload:
        return "OK", 200

    events = (
        payload.get("events")
        or []
    )

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
# 이벤트 처리
# =========================================================

def process_event(event):

    started = time.monotonic()

    if (
        event.get("type")
        != "message"
    ):
        return

    message = (
        event.get("message")
        or {}
    )

    if (
        message.get("type")
        != "text"
    ):
        return

    source = (
        event.get("source")
        or {}
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

    event_id = message.get(
        "id"
    )

    raw_text = message.get(
        "text",
        ""
    )

    text = raw_text.strip()

    reply_token = event.get(
        "replyToken"
    )

    if (
        not group_id
        or not user_id
        or not event_id
    ):
        return

    today = datetime.now(
        KST
    ).date()

    command = is_command(
        text
    )

    # 프로필 캐시가 있으면 즉시 사용
    display_name = get_display_name(
        source,
        group_id,
        user_id
    )

    # 공백 제외 글자 수
    character_count = len(
        re.sub(
            r"\s+",
            "",
            raw_text
        )
    )

    # -------------------------------------
    # 모든 메시지는 마딧수 기록
    # -------------------------------------

    try:

        record_message(
            event_id,
            group_id,
            user_id,
            display_name,
            today,
            character_count
        )

    except Exception:

        logger.exception(
            "메시지 기록 실패"
        )

    # -------------------------------------
    # 일반 대화는 여기서 끝
    # -------------------------------------

    if not command:

        schedule_profile_lookup(
            source,
            group_id,
            user_id
        )

        return

    # -------------------------------------
    # 명령어
    # -------------------------------------

    try:

        # 명령어일 때 이름이 아직
        # 임시 이름이면 한 번 직접 조회
        if (
            display_name.startswith(
                "사용자-"
            )
            or display_name
            == "이름 미확인"
        ):

            fetched_name = (
                get_display_name(
                    source,
                    group_id,
                    user_id,
                    force_fetch=True
                )
            )

            if fetched_name:
                display_name = (
                    fetched_name
                )

        reply = handle_command(

            text,

            source,

            group_id,

            user_id,

            today,

            display_name,

            reply_token
        )

        if (
            reply
            and reply
            != "__MENTION_SENT__"
        ):

            sent = reply_text(
                reply_token,
                reply
            )

            if not sent:

                logger.error(
                    "명령어 응답 전송 실패: %r",
                    text
                )

    except Exception:

        logger.exception(
            "명령어 실행 실패: %r",
            text
        )

        # 어떤 오류가 나더라도
        # 사용자에게 완전한 빈 응답만 남지 않게 한다.
        try:

            reply_text(
                reply_token,
                (
                    "어라... 방금 "
                    "장난감 부품이 하나 빠졌어 🥄💦\n"
                    "잠깐 있다가 다시 불러줘!"
                )
            )

        except Exception:

            logger.exception(
                "오류 안내 메시지 전송 실패"
            )

    finally:

        schedule_profile_lookup(
            source,
            group_id,
            user_id
        )

        elapsed = (
            time.monotonic()
            - started
        )

        if elapsed >= 2:

            logger.warning(
                "명령어 처리 지연: %.2fs command=%r",
                elapsed,
                text
            )


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    try:

        init_database()

    except Exception:

        logger.warning(
            "시작 시 DB 초기화 실패",
            exc_info=True
        )

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
