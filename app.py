import os
import hmac
import hashlib
import base64
import logging
import re
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import psycopg
from psycopg.rows import dict_row
from flask import Flask, request, jsonify, abort


# ==================================================
# 기본 설정
# ==================================================

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("baby-factory-maditsu-bot")

LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
KST = ZoneInfo("Asia/Seoul")

http = requests.Session()
_db_init_lock = threading.Lock()
_db_initialized = False

# 프로필 조회 결과를 잠시 캐시해 명령어 응답 속도를 개선합니다.
_PROFILE_CACHE = {}
_PROFILE_CACHE_LOCK = threading.Lock()
_PROFILE_CACHE_TTL = 600  # 10분


# ==================================================
# 데이터베이스 연결 및 초기화
# ==================================================

def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL 환경변수가 설정되지 않았습니다.")
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=8,
    )


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
# LINE 프로필 및 API
# ==================================================

def line_headers():
    return {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def get_display_name(source, group_id, user_id):
    """LINE 프로필을 조회합니다. 조회 결과는 10분간 캐시합니다."""
    fallback_name = "사용자-" + user_id[-6:]
    if not LINE_CHANNEL_ACCESS_TOKEN:
        return fallback_name

    cache_key = (source.get("type"), group_id, user_id)
    now_ts = time.time()
    with _PROFILE_CACHE_LOCK:
        cached = _PROFILE_CACHE.get(cache_key)
        if cached and now_ts - cached[0] < _PROFILE_CACHE_TTL:
            return cached[1]

    source_type = source.get("type")
    if source_type == "group" and group_id:
        url = f"https://api.line.me/v2/bot/group/{group_id}/member/{user_id}"
    elif source_type == "room" and group_id:
        url = f"https://api.line.me/v2/bot/room/{group_id}/member/{user_id}"
    else:
        url = f"https://api.line.me/v2/bot/profile/{user_id}"

    try:
        response = http.get(url, headers=line_headers(), timeout=3)
        if response.ok:
            name = response.json().get("displayName")
            if name:
                with _PROFILE_CACHE_LOCK:
                    _PROFILE_CACHE[cache_key] = (now_ts, name)
                return name
        logger.warning("프로필 조회 실패: status=%s", response.status_code)
    except requests.RequestException:
        logger.exception("LINE 프로필 조회 중 오류 발생")

    with _PROFILE_CACHE_LOCK:
        _PROFILE_CACHE[cache_key] = (now_ts, fallback_name)
    return fallback_name


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
                "messages": [{"type": "text", "text": str(text)[:4900]}],
            },
            timeout=5,
        )
        if not response.ok:
            logger.error(
                "LINE 답장 실패: status=%s body=%s",
                response.status_code, response.text[:800]
            )
    except requests.RequestException:
        logger.exception("LINE 답장 전송 중 오류 발생")


def reply_mention(reply_token, text, members):
    """LINE textV2 형식으로 등록된 인증자를 실제 멘션합니다."""
    if not reply_token or not LINE_CHANNEL_ACCESS_TOKEN:
        logger.error("멘션 답장 중단: replyToken 또는 LINE 액세스 토큰이 없습니다.")
        return
    if not members:
        reply_text(reply_token, text)
        return

    # LINE textV2는 {키} 자리에 mention substitution을 넣어야 실제 멘션이 됩니다.
    # 일반 text 메시지의 mention 필드는 발신 메시지에서 실제 멘션을 만드는 방식이 아닙니다.
    valid_members = [
        member for member in members
        if member.get("user_id") and str(member["user_id"]).startswith("U")
    ][:20]  # LINE은 한 메시지에 최대 20명까지 멘션할 수 있습니다.

    if not valid_members:
        logger.error("멘션 답장 실패: 유효한 인증자 user_id가 없습니다.")
        reply_text(reply_token, text + "\n인증자 사용자 정보를 확인해 주세요.")
        return

    substitution = {}
    placeholders = []
    for index, member in enumerate(valid_members, start=1):
        key = f"auth{index}"
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
        "messages": [{
            "type": "textV2",
            "text": message_text[:4900],
            "substitution": substitution,
        }],
    }

    try:
        response = http.post(
            "https://api.line.me/v2/bot/message/reply",
            headers=line_headers(),
            json=payload,
            timeout=5,
        )
        if not response.ok:
            logger.error(
                "LINE 실제 멘션 답장 실패: status=%s body=%s payload=%s",
                response.status_code, response.text[:1000], payload,
            )
    except requests.RequestException:
        logger.exception("LINE 실제 멘션 답장 전송 중 오류 발생")


# ==================================================
# 메시지 기록
# ==================================================

def record_message(event_id, group_id, user_id, display_name, message_date, character_count):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO message_events
                (event_id, group_id, user_id, display_name, message_date, character_count)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (event_id, group_id, user_id, display_name, message_date, character_count),
        )


# ==================================================
# 인증자 등록/해제
# ==================================================

def register_authenticator(group_id, user_id, display_name):
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO group_authenticators (group_id, user_id, display_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (group_id, user_id)
            DO UPDATE SET display_name = EXCLUDED.display_name,
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
            INSERT INTO group_admins (group_id, user_id, display_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (group_id, user_id)
            DO UPDATE SET display_name = EXCLUDED.display_name,
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
# 오늘의 개인 마딧수
# ==================================================

def get_my_stats(group_id, user_id, today, display_name):
    # 개인 수치와 순위를 한 번의 SQL로 조회해 DB 왕복을 줄입니다.
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
                WHERE group_id = %s AND message_date = %s
                GROUP BY user_id
            ),
            ranked AS (
                SELECT
                    user_id,
                    message_count,
                    character_count,
                    RANK() OVER (ORDER BY message_count DESC) AS user_rank
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


# ==================================================
# 전체 마딧수 순위
# ==================================================

def get_ranking(group_id, start_date, end_date, title):
    with get_db() as conn:
        # 동일한 범위의 집계에서 순위와 총계를 함께 가져옵니다.
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
            ORDER BY message_count DESC, LOWER(MAX(display_name)) ASC
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
            f"{rank}위. {row['display_name']} - {row['message_count']:,}마디"
        )
    lines.extend([
        "",
        "※ 순위는 최대 50명까지 표시됩니다.",
        "※ 동점자는 표시 순서에 따라 번호가 달라질 수 있습니다.",
    ])
    return "\n".join(lines)


# ==================================================
# 명령어 처리
# ==================================================

def handle_command(text, group_id, user_id, today, display_name, reply_token):
    if text == "내 마딧수":
        return get_my_stats(group_id, user_id, today, display_name)

    if text == "*마딧수":
        return get_ranking(group_id, today, today, str(today))

    if text == "인증자 등록":
        register_authenticator(group_id, user_id, display_name)
        return f"{display_name}님이 인증자로 등록되었습니다."

    if text == "인증자 해제":
        removed = unregister_authenticator(group_id, user_id)
        if removed:
            return f"{display_name}님이 인증자에서 해제되었습니다."
        return f"{display_name}님은 현재 인증자 명단에 등록되어 있지 않습니다."

    if text == "관리자 등록":
        register_admin(group_id, user_id, display_name)
        return f"{display_name}님이 관리자로 등록되었습니다."

    if text == "관리자 해제":
        removed = unregister_admin(group_id, user_id)
        if removed:
            return f"{display_name}님이 관리자에서 해제되었습니다."
        return f"{display_name}님은 현재 관리자 명단에 등록되어 있지 않습니다."

    if text in ("*벙시", "*벙종"):
        admins = get_admins(group_id)
        if not admins:
            return (
                "등록된 관리자가 없습니다.\n"
                "관리자로 등록할 분은 '관리자 등록'을 입력해 주세요."
            )
        message_text = "벙시요~" if text == "*벙시" else "벙종이요~"
        reply_mention(reply_token, message_text, admins)
        return "__MENTION_SENT__"

    if text in ("*여초1", "*남초1"):
        members = get_authenticators(group_id)
        if not members:
            return (
                "등록된 인증자가 없습니다.\n"
                "인증자로 참여할 분은 '인증자 등록'을 입력해 주세요."
            )
        reply_mention(reply_token, text, members)
        return "__MENTION_SENT__"

    match = re.fullmatch(
        r"\*(\d{4}-\d{2}-\d{2})\s*~\s*(\d{4}-\d{2}-\d{2})\s*마딧수",
        text,
    )
    if match:
        try:
            start_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
            end_date = datetime.strptime(match.group(2), "%Y-%m-%d").date()
        except ValueError:
            return (
                "날짜 형식이 올바르지 않습니다.\n"
                "예시: *2026-11-01~2026-11-30 마딧수"
            )

        if start_date > end_date:
            return "시작 날짜가 종료 날짜보다 늦습니다."

        return get_ranking(
            group_id, start_date, end_date, f"{start_date} ~ {end_date}"
        )

    return None


# ==================================================
# 상태 확인 주소
# ==================================================

@app.route("/", methods=["GET"])
def home():
    return "Baby Factory 마딧수 봇이 실행 중입니다."


@app.route("/health", methods=["GET"])
def health():
    try:
        with get_db() as conn:
            conn.execute("SELECT 1")
        return jsonify({"status": "ok", "database": "connected"})
    except Exception:
        logger.exception("헬스 체크 실패")
        return jsonify({"status": "error", "database": "disconnected"}), 500


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
        LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256
    ).digest()
    expected_signature = base64.b64encode(digest).decode("utf-8")

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
    text = message.get("text", "")

    if not group_id or not user_id or not message_id:
        logger.warning("필수 이벤트 정보가 없어 건너뜁니다.")
        return

    today = datetime.now(KST).date()
    display_name = get_display_name(source, group_id, user_id)
    character_count = len(re.sub(r"\s+", "", text))

    # 일반 대화와 명령어 메시지를 모두 기존 방식대로 기록합니다.
    record_message(
        event_id=message_id,
        group_id=group_id,
        user_id=user_id,
        display_name=display_name,
        message_date=today,
        character_count=character_count,
    )

    reply = handle_command(
        text.strip(),
        group_id,
        user_id,
        today,
        display_name,
        event.get("replyToken"),
    )

    # 멘션 명령은 handle_command 내부에서 이미 답장했습니다.
    if reply and reply != "__MENTION_SENT__":
        reply_text(event.get("replyToken"), reply)


# ==================================================
# 직접 실행용
# Render 실행 명령: gunicorn app:app
# ==================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
