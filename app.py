
import os
import hmac
import hashlib
import base64
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from flask import Flask, request, jsonify, abort


# ==================================================
# 기본 설정
# ==================================================

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("baby-factory-maditsu-bot")

LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
LINE_CHANNEL_ACCESS_TOKEN = os.getenv(
    "LINE_CHANNEL_ACCESS_TOKEN", ""
).strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

KST = ZoneInfo("Asia/Seoul")

http = requests.Session()
http.mount(
    "https://",
    HTTPAdapter(pool_connections=4, pool_maxsize=8),
)

_db_init_lock = threading.Lock()
_db_initialized = False

# Neon Pooled connection 주소를 DATABASE_URL에 사용합니다.
# 앱 내부 연결 풀의 대기 시간을 늘리고 연결 개수를 제한합니다.
_db_pool = None

if DATABASE_URL:
    _db_pool = ConnectionPool(
        conninfo=DATABASE_URL,
        min_size=1,
        max_size=8,
        timeout=10,
        kwargs={
            "row_factory": dict_row,
            "connect_timeout": 5,
        },
        open=True,
    )

# 프로필 조회가 DB 연결을 과도하게 점유하지 않도록
# 백그라운드 작업자를 4명에서 2명으로 줄입니다.
_PROFILE_CACHE = {}
_PROFILE_CACHE_LOCK = threading.Lock()
_PROFILE_CACHE_TTL = 600
_PROFILE_LOOKUPS_IN_FLIGHT = set()

_PROFILE_EXECUTOR = ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="line-profile",
)


# ==================================================
# 데이터베이스 연결
# ==================================================

def get_db():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL 환경변수가 설정되지 않았습니다."
        )

    if _db_pool is None:
        raise RuntimeError(
            "데이터베이스 연결 풀이 초기화되지 않았습니다."
        )

    return _db_pool.connection(timeout=10)


def init_database():
    global _db_initialized

    if _db_initialized:
        return

    with _db_init_lock:
        if _db_initialized:
            return

        with get_db() as conn:
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
                CREATE INDEX IF NOT EXISTS idx_message_group_date
                ON message_events (group_id, message_date)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_message_group_user_date
                ON message_events (group_id, user_id, message_date)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_message_group_date_user
                ON message_events (group_id, message_date, user_id)
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS group_authenticators (
                    group_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '이름 미확인',
                    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (group_id, user_id)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS group_admins (
                    group_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '이름 미확인',
                    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (group_id, user_id)
                )
            """)

        _db_initialized = True
        logger.info("데이터베이스 초기화 완료")


init_database()


# ==================================================
# LINE 프로필
# ==================================================

def line_headers():
    return {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def _profile_url(source, group_id, user_id):
    source_type = source.get("type")

    if source_type == "group" and group_id:
        return (
            f"https://api.line.me/v2/bot/group/"
            f"{group_id}/member/{user_id}"
        )

    if source_type == "room" and group_id:
        return (
            f"https://api.line.me/v2/bot/room/"
            f"{group_id}/member/{user_id}"
        )

    return f"https://api.line.me/v2/bot/profile/{user_id}"


def _fetch_profile_name(source, group_id, user_id, cache_key):
    """프로필을 백그라운드에서 조회하고 임시 이름을 보정합니다."""
    try:
        if not LINE_CHANNEL_ACCESS_TOKEN:
            return

        response = http.get(
            _profile_url(source, group_id, user_id),
            headers=line_headers(),
            timeout=(2, 3),
        )

        if not response.ok:
            logger.warning(
                "프로필 조회 실패: status=%s",
                response.status_code,
            )
            return

        name = response.json().get("displayName")

        if not name:
            return

        with _PROFILE_CACHE_LOCK:
            _PROFILE_CACHE[cache_key] = (time.time(), name)

        fallback_name = "사용자-" + user_id[-6:]

        # 이 작업은 백그라운드에서만 실행합니다.
        # DB 오류가 나더라도 메시지 명령어 처리에는 영향을 주지 않습니다.
        try:
            with get_db() as conn:
                conn.execute(
                    """
                    UPDATE message_events
                    SET display_name = %s
                    WHERE group_id = %s
                      AND user_id = %s
                      AND display_name = %s
                    """,
                    (name, group_id, user_id, fallback_name),
                )

                conn.execute(
                    """
                    UPDATE group_authenticators
                    SET display_name = %s
                    WHERE group_id = %s
                      AND user_id = %s
                      AND display_name = %s
                    """,
                    (name, group_id, user_id, fallback_name),
                )

                conn.execute(
                    """
                    UPDATE group_admins
                    SET display_name = %s
                    WHERE group_id = %s
                      AND user_id = %s
                      AND display_name = %s
                    """,
                    (name, group_id, user_id, fallback_name),
                )

        except Exception:
            logger.exception("프로필 이름 DB 갱신 실패")

    except requests.RequestException:
        logger.warning(
            "프로필 조회 네트워크 오류",
            exc_info=True,
        )

    except Exception:
        logger.exception("프로필 처리 중 오류")

    finally:
        with _PROFILE_CACHE_LOCK:
            _PROFILE_LOOKUPS_IN_FLIGHT.discard(cache_key)


def get_display_name(source, group_id, user_id):
    fallback_name = "사용자-" + user_id[-6:]
    cache_key = (source.get("type"), group_id, user_id)

    with _PROFILE_CACHE_LOCK:
        cached = _PROFILE_CACHE.get(cache_key)

        if cached and time.time() - cached[0] < _PROFILE_CACHE_TTL:
            return cached[1]

    return fallback_name


def schedule_profile_lookup(source, group_id, user_id):
    if not LINE_CHANNEL_ACCESS_TOKEN:
        return

    cache_key = (source.get("type"), group_id, user_id)

    with _PROFILE_CACHE_LOCK:
        cached = _PROFILE_CACHE.get(cache_key)

        if cached and time.time() - cached[0] < _PROFILE_CACHE_TTL:
            return

        if cache_key in _PROFILE_LOOKUPS_IN_FLIGHT:
            return

        _PROFILE_LOOKUPS_IN_FLIGHT.add(cache_key)

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
            _PROFILE_LOOKUPS_IN_FLIGHT.discard(cache_key)

        logger.exception("프로필 조회 작업 예약 실패")


# ==================================================
# LINE 답장 및 실제 멘션
# ==================================================

def reply_text(reply_token, text):
    if not reply_token:
        return

    if not LINE_CHANNEL_ACCESS_TOKEN:
        logger.error("LINE 액세스 토큰이 설정되지 않았습니다.")
        return

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
            timeout=(3, 5),
        )

        if not response.ok:
            logger.error(
                "LINE 답장 실패: status=%s body=%s",
                response.status_code,
                response.text[:800],
            )

    except requests.RequestException:
        logger.exception("LINE 답장 전송 중 오류")


def reply_mention(reply_token, text, members):
    if not reply_token or not LINE_CHANNEL_ACCESS_TOKEN:
        logger.error("멘션 답장에 필요한 설정이 없습니다.")
        return

    if not members:
        reply_text(reply_token, text)
        return

    valid_members = [
        member for member in members
        if member.get("user_id")
        and str(member["user_id"]).startswith("U")
    ][:20]

    if not valid_members:
        reply_text(
            reply_token,
            text + "\n등록된 사용자 정보를 확인해 주세요.",
        )
        return

    substitution = {}
    placeholders = []

    for index, member in enumerate(valid_members, start=1):
        key = f"member{index}"
        placeholders.append("{" + key + "}")

        substitution[key] = {
            "type": "mention",
            "mentionee": {
                "type": "user",
                "userId": member["user_id"],
            },
        }

    message_text = str(text).rstrip() + "\n" + " ".join(placeholders)

    payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "textV2",
                "text": message_text[:4900],
                "substitution": substitution,
            }
        ],
    }

    try:
        response = http.post(
            "https://api.line.me/v2/bot/message/reply",
            headers=line_headers(),
            json=payload,
            timeout=(3, 5),
        )

        if not response.ok:
            logger.error(
                "LINE 멘션 실패: status=%s body=%s",
                response.status_code,
                response.text[:1000],
            )

    except requests.RequestException:
        logger.exception("LINE 멘션 전송 중 오류")


# ==================================================
# 메시지 기록
# ==================================================

def record_message(
    event_id,
    group_id,
    user_id,
    display_name,
    message_date,
    character_count,
):
    started = time.monotonic()

    with get_db() as conn:
        conn.execute(
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
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
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

    elapsed = time.monotonic() - started

    if elapsed >= 1:
        logger.warning("메시지 DB 기록 지연: %.2f초", elapsed)


# ==================================================
# 인증자 등록/해제
# ==================================================

def register_authenticator(group_id, user_id, display_name):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO group_authenticators
                (group_id, user_id, display_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (group_id, user_id)
            DO UPDATE SET
                display_name = EXCLUDED.display_name,
                registered_at = NOW()
            """,
            (group_id, user_id, display_name),
        )


def unregister_authenticator(group_id, user_id):
    with get_db() as conn:
        cur = conn.execute(
            """
            DELETE FROM group_authenticators
            WHERE group_id = %s AND user_id = %s
            """,
            (group_id, user_id),
        )

        return cur.rowcount


def get_authenticators(group_id):
    with get_db() as conn:
        return conn.execute(
            """
            SELECT user_id, display_name
            FROM group_authenticators
            WHERE group_id = %s
            ORDER BY registered_at ASC
            """,
            (group_id,),
        ).fetchall()


# ==================================================
# 관리자 등록/해제
# ==================================================

def register_admin(group_id, user_id, display_name):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO group_admins
                (group_id, user_id, display_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (group_id, user_id)
            DO UPDATE SET
                display_name = EXCLUDED.display_name,
                registered_at = NOW()
            """,
            (group_id, user_id, display_name),
        )


def unregister_admin(group_id, user_id):
    with get_db() as conn:
        cur = conn.execute(
            """
            DELETE FROM group_admins
            WHERE group_id = %s AND user_id = %s
            """,
            (group_id, user_id),
        )

        return cur.rowcount


def get_admins(group_id):
    with get_db() as conn:
        return conn.execute(
            """
            SELECT user_id, display_name
            FROM group_admins
            WHERE group_id = %s
            ORDER BY registered_at ASC
            """,
            (group_id,),
        ).fetchall()


# ==================================================
# 개인 마딧수 및 순위
# ==================================================

def get_my_stats(group_id, user_id, today, display_name):
    with get_db() as conn:
        row = conn.execute(
            """
            WITH daily_counts AS (
                SELECT
                    user_id,
                    MAX(display_name) AS display_name,
                    COUNT(*) AS message_count,
                    COALESCE(SUM(character_count), 0) AS character_count
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
            SELECT message_count, character_count, user_rank
            FROM ranked
            WHERE user_id = %s
            """,
            (group_id, today, user_id),
        ).fetchone()

    if row:
        count = row["message_count"]
        chars = row["character_count"]
        rank = row["user_rank"]
    else:
        count, chars, rank = 0, 0, "-"

    return (
        f"오늘의 마딧수\n"
        f"이름: {display_name}\n"
        f"날짜: {today}\n"
        f"내 마딧수: {count:,}마디\n"
        f"공백 제외 글자 수: {chars:,}자\n"
        f"오늘의 순위: {rank}위"
    )


def get_ranking(group_id, start_date, end_date, title):
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT
                user_id,
                MAX(display_name) AS display_name,
                COUNT(*) AS message_count
            FROM message_events
            WHERE group_id = %s
              AND message_date >= %s
              AND message_date <= %s
            GROUP BY user_id
            ORDER BY
                message_count DESC,
                LOWER(MAX(display_name)) ASC
            LIMIT 50
            """,
            (group_id, start_date, end_date),
        ).fetchall()

        summary = conn.execute(
            """
            SELECT
                COUNT(*) AS total_messages,
                COUNT(DISTINCT user_id) AS participant_count
            FROM message_events
            WHERE group_id = %s
              AND message_date >= %s
              AND message_date <= %s
            """,
            (group_id, start_date, end_date),
        ).fetchone()

    lines = [
        f"마딧수 순위: {title}",
        f"전체 메시지: {summary['total_messages']:,}개",
        f"참여 인원: {summary['participant_count']:,}명",
        "",
    ]

    if not rows:
        lines.extend([
            "아직 기록된 메시지가 없습니다.",
            "봇이 들어온 뒤 기록된 메시지부터 집계됩니다.",
        ])
        return "\n".join(lines)

    lines.append("활동 순위")

    for rank, row in enumerate(rows, start=1):
        lines.append(
            f"{rank}위. {row['display_name']} - "
            f"{row['message_count']:,}마디"
        )

    lines.extend([
        "",
        "※ 순위는 최대 50명까지 표시됩니다.",
        "※ 동점자는 표시 순서에 따라 번호가 달라질 수 있습니다.",
    ])

    return "\n".join(lines)


# ==================================================
# 명령어
# ==================================================

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
}

RANGE_COMMAND_RE = re.compile(
    r"\*(\d{4}-\d{2}-\d{2})\s*~\s*"
    r"(\d{4}-\d{2}-\d{2})\s*마딧수"
)


def is_command(text):
    normalized = text.strip()

    return (
        normalized in EXACT_COMMANDS
        or bool(RANGE_COMMAND_RE.fullmatch(normalized))
    )


def handle_command(
    text,
    group_id,
    user_id,
    today,
    display_name,
    reply_token,
):
    if text == "내 마딧수":
        return get_my_stats(
            group_id, user_id, today, display_name
        )

    if text == "*마딧수":
        return get_ranking(
            group_id, today, today, str(today)
        )

    if text == "인증자 등록":
        register_authenticator(
            group_id, user_id, display_name
        )
        return f"{display_name}님이 인증자로 등록되었습니다."

    if text == "인증자 해제":
        removed = unregister_authenticator(group_id, user_id)

        if removed:
            return f"{display_name}님이 인증자에서 해제되었습니다."

        return (
            f"{display_name}님은 현재 인증자 명단에 "
            "등록되어 있지 않습니다."
        )

    if text == "관리자 등록":
        register_admin(group_id, user_id, display_name)
        return f"{display_name}님이 관리자로 등록되었습니다."

    if text == "관리자 해제":
        removed = unregister_admin(group_id, user_id)

        if removed:
            return f"{display_name}님이 관리자에서 해제되었습니다."

        return (
            f"{display_name}님은 현재 관리자 명단에 "
            "등록되어 있지 않습니다."
        )

    if text in ("*벙시", "*벙종"):
        admins = get_admins(group_id)

        if not admins:
            return (
                "등록된 관리자가 없습니다.\n"
                "관리자로 등록할 분은 '관리자 등록'을 입력해 주세요."
            )

        message_text = (
            "벙시요~" if text == "*벙시" else "벙종이요~"
        )

        reply_mention(reply_token, message_text, admins)
        return "__MENTION_SENT__"

    if text in ("*여초", "*남초", "*여초1", "*남초1"):
        members = get_authenticators(group_id)

        if not members:
            return (
                "등록된 인증자가 없습니다.\n"
                "인증자로 참여할 분은 '인증자 등록'을 입력해 주세요."
            )

        reply_mention(reply_token, text, members)
        return "__MENTION_SENT__"

    match = RANGE_COMMAND_RE.fullmatch(text)

    if match:
        try:
            start_date = datetime.strptime(
                match.group(1), "%Y-%m-%d"
            ).date()

            end_date = datetime.strptime(
                match.group(2), "%Y-%m-%d"
            ).date()

        except ValueError:
            return (
                "날짜 형식이 올바르지 않습니다.\n"
                "예시: *2026-11-01~2026-11-30 마딧수"
            )

        if start_date > end_date:
            return "시작 날짜가 종료 날짜보다 늦습니다."

        return get_ranking(
            group_id,
            start_date,
            end_date,
            f"{start_date} ~ {end_date}",
        )

    return None


# ==================================================
# 상태 확인
# ==================================================

@app.route("/", methods=["GET"])
def home():
    return "Baby Factory 마딧수 봇이 실행 중입니다."


@app.route("/health", methods=["GET"])
def health():
    try:
        with get_db() as conn:
            conn.execute("SELECT 1")

        return jsonify({
            "status": "ok",
            "database": "connected",
        })

    except Exception:
        logger.exception("헬스 체크 실패")

        return jsonify({
            "status": "error",
            "database": "disconnected",
        }), 500


# ==================================================
# LINE 웹훅
# ==================================================

@app.route("/callback", methods=["POST"])
def callback():
    body = request.get_data()
    signature = request.headers.get("X-Line-Signature", "")

    if not LINE_CHANNEL_SECRET:
        logger.error("LINE_CHANNEL_SECRET이 설정되지 않았습니다.")
        abort(500)

    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"),
        body,
        hashlib.sha256,
    ).digest()

    expected_signature = base64.b64encode(
        digest
    ).decode("utf-8")

    if not hmac.compare_digest(expected_signature, signature):
        logger.warning("LINE 웹훅 서명 검증 실패")
        abort(400)

    try:
        payload = request.get_json(force=True)

        for event in payload.get("events", []):
            try:
                process_event(event)

            except Exception:
                logger.exception("LINE 이벤트 처리 실패")

        return "OK", 200

    except Exception:
        logger.exception("웹훅 처리 중 오류 발생")
        return "ERROR", 500


# ==================================================
# 개별 이벤트 처리
# ==================================================

def process_event(event):
    started_at = time.monotonic()

    if event.get("type") != "message":
        return

    message = event.get("message", {})

    if message.get("type") != "text":
        return

    source = event.get("source", {})
    source_type = source.get("type")

    if source_type == "group":
        group_id = source.get("groupId")
    elif source_type == "room":
        group_id = source.get("roomId")
    else:
        return

    user_id = source.get("userId")
    message_id = message.get("id")
    raw_text = message.get("text", "")
    normalized_text = raw_text.strip()
    reply_token = event.get("replyToken")

    if not group_id or not user_id or not message_id:
        logger.warning("필수 이벤트 정보가 없어 건너뜁니다.")
        return

    today = datetime.now(KST).date()

    display_name = get_display_name(
        source, group_id, user_id
    )

    character_count = len(
        re.sub(r"\s+", "", raw_text)
    )

    # 핵심 수정:
    # 메시지 기록이 실패하더라도 명령어 처리까지 중단하지 않습니다.
    # 다만 DB 장애 중에는 해당 메시지 기록이 누락될 수 있습니다.
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
            "메시지 기록 실패: group_id=%s user_id=%s",
            group_id,
            user_id,
        )

    # 일반 채팅은 기록 후 종료합니다.
    if not is_command(normalized_text):
        schedule_profile_lookup(
            source, group_id, user_id
        )

        elapsed = time.monotonic() - started_at

        if elapsed >= 1:
            logger.warning(
                "일반 메시지 처리 지연: %.2f초",
                elapsed,
            )

        return

    # 명령어 처리 중 DB 오류가 발생하면 로그를 남기고
    # 사용자에게 오류 안내를 시도합니다.
    try:
        reply = handle_command(
            normalized_text,
            group_id,
            user_id,
            today,
            display_name,
            reply_token,
        )

        if reply and reply != "__MENTION_SENT__":
            reply_text(reply_token, reply)

    except Exception:
        logger.exception(
            "명령어 실행 실패: command=%r group_id=%s",
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
            source, group_id, user_id
        )

    elapsed = time.monotonic() - started_at

    if elapsed >= 1:
        logger.warning(
            "명령어 처리 지연: %.2f초, 명령어=%r",
            elapsed,
            normalized_text,
        )


# ==================================================
# 직접 실행
# Render 실행 명령: gunicorn app:app
# ==================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
    )
