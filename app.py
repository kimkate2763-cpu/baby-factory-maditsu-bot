import os
import hmac
import hashlib
import base64
import logging
import re

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
LINE_CHANNEL_ACCESS_TOKEN = os.getenv(
    "LINE_CHANNEL_ACCESS_TOKEN", ""
).strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

KST = ZoneInfo("Asia/Seoul")


# ==================================================
# 데이터베이스 연결
# ==================================================

def get_db():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL 환경변수가 설정되지 않았습니다."
        )

    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=15,
    )


# ==================================================
# 데이터베이스 테이블 자동 생성
# ==================================================

def init_database():
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

    logger.info("데이터베이스 초기화 완료")


# 중요: Gunicorn으로 실행해도 테이블이 생성되도록
# 파일을 불러오는 시점에 초기화를 실행합니다.
init_database()


# ==================================================
# LINE 사용자 프로필 및 닉네임
# ==================================================

def get_display_name(source, group_id, user_id):
    """LINE 프로필을 조회하고, 조회 실패 시 기본 이름을 사용합니다."""

    fallback_name = "사용자-" + user_id[-6:]

    if not LINE_CHANNEL_ACCESS_TOKEN:
        return fallback_name

    if source.get("type") == "group" and group_id:
        url = (
            "https://api.line.me/v2/bot/group/"
            f"{group_id}/member/{user_id}"
        )
    elif source.get("type") == "room" and group_id:
        url = (
            "https://api.line.me/v2/bot/room/"
            f"{group_id}/member/{user_id}"
        )
    else:
        url = (
            "https://api.line.me/v2/bot/profile/"
            f"{user_id}"
        )

    try:
        response = requests.get(
            url,
            headers={
                "Authorization": (
                    f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
                )
            },
            timeout=8,
        )

        if response.ok:
            name = response.json().get("displayName")
            if name:
                return name

        logger.warning(
            "프로필 조회 실패: status=%s",
            response.status_code,
        )

    except Exception:
        logger.exception("LINE 프로필 조회 중 오류 발생")

    return fallback_name


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
    """같은 메시지가 중복으로 기록되지 않도록 저장합니다."""

    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO message_events (
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


# ==================================================
# LINE 답장
# ==================================================

def reply_text(reply_token, text):
    if not reply_token:
        return

    if not LINE_CHANNEL_ACCESS_TOKEN:
        logger.error("LINE 액세스 토큰이 설정되지 않았습니다.")
        return

    try:
        response = requests.post(
            "https://api.line.me/v2/bot/message/reply",
            headers={
                "Authorization": (
                    f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
                ),
                "Content-Type": "application/json",
            },
            json={
                "replyToken": reply_token,
                "messages": [
                    {
                        "type": "text",
                        "text": str(text)[:4900],
                    }
                ],
            },
            timeout=10,
        )

        if not response.ok:
            logger.error(
                "LINE 답장 실패: status=%s body=%s",
                response.status_code,
                response.text[:1000],
            )

    except Exception:
        logger.exception("LINE 답장 전송 중 오류 발생")


# ==================================================
# 오늘의 개인 마딧수
# ==================================================

def get_my_stats(group_id, user_id, today):
    with get_db() as conn:
        stats = conn.execute(
            """
            SELECT
                COUNT(*) AS message_count,
                COALESCE(SUM(character_count), 0)
                    AS character_count
            FROM message_events
            WHERE group_id = %s
              AND user_id = %s
              AND message_date = %s
            """,
            (group_id, user_id, today),
        ).fetchone()

        ranking = conn.execute(
            """
            WITH daily_counts AS (
                SELECT
                    user_id,
                    COUNT(*) AS message_count
                FROM message_events
                WHERE group_id = %s
                  AND message_date = %s
                GROUP BY user_id
            ),
            ranked AS (
                SELECT
                    user_id,
                    message_count,
                    RANK() OVER (
                        ORDER BY message_count DESC
                    ) AS user_rank
                FROM daily_counts
            )
            SELECT user_rank
            FROM ranked
            WHERE user_id = %s
            """,
            (group_id, today, user_id),
        ).fetchone()

    rank = ranking["user_rank"] if ranking else "-"

    return (
        f"오늘의 마딧수\n"
        f"날짜: {today}\n"
        f"내 마딧수: {stats['message_count']:,}마디\n"
        f"공백 제외 글자 수: {stats['character_count']:,}자\n"
        f"오늘의 순위: {rank}위"
    )


# ==================================================
# 전체 마딧수 순위
# ==================================================

def get_ranking(group_id, start_date, end_date, title):
    with get_db() as conn:
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
            ORDER BY message_count DESC, display_name ASC
            LIMIT 50
            """,
            (group_id, start_date, end_date),
        ).fetchall()

    lines = [
        f"마딧수 순위: {title}",
        f"전체 메시지: {summary['total_messages']:,}개",
        f"참여 인원: {summary['participant_count']:,}명",
        "",
    ]

    if not rows:
        lines.append("아직 기록된 메시지가 없습니다.")
        lines.append("봇이 들어온 뒤 기록된 메시지부터 집계됩니다.")
        return "\n".join(lines)

    lines.append("활동 순위")

    for rank, row in enumerate(rows, start=1):
        lines.append(
            f"{rank}위. {row['display_name']} "
            f"- {row['message_count']:,}마디"
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

def handle_command(text, group_id, user_id, today):
    if text == "내 마딧수":
        return get_my_stats(group_id, user_id, today)

    if text == "*마딧수":
        return get_ranking(
            group_id,
            today,
            today,
            str(today),
        )

    match = re.fullmatch(
        r"\*(\d{4}-\d{2}-\d{2})\s*~\s*"
        r"(\d{4}-\d{2}-\d{2})\s*마딧수",
        text,
    )

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
                "예시: *2026-10-01~2026-10-31 마딧수"
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

    if not hmac.compare_digest(
        expected_signature, signature
    ):
        logger.warning("LINE 웹훅 서명 검증 실패")
        abort(400)

    try:
        payload = request.get_json(force=True)
        events = payload.get("events", [])

        for event in events:
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

    # 텍스트 메시지만 집계합니다.
    if message.get("type") != "text":
        return

    source = event.get("source", {})
    source_type = source.get("type")

    # 그룹 또는 LINE 멀티인원 대화만 집계합니다.
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

    now = datetime.now(KST)
    today = now.date()

    display_name = get_display_name(
        source,
        group_id,
        user_id,
    )

    # 공백과 줄바꿈을 제외한 글자 수를 계산합니다.
    character_count = len(
        re.sub(r"\s+", "", text)
    )

    # 먼저 메시지를 저장합니다. 동일한 LINE 메시지는 중복 저장하지 않습니다.
    record_message(
        event_id=message_id,
        group_id=group_id,
        user_id=user_id,
        display_name=display_name,
        message_date=today,
        character_count=character_count,
    )

    # 아래 명령어에만 답장합니다. 일반 대화에는 답장하지 않습니다.
    reply = handle_command(
        text.strip(),
        group_id,
        user_id,
        today,
    )

    if reply:
        reply_text(
            event.get("replyToken"),
            reply,
        )


# ==================================================
# 직접 실행용
# Render에서는 gunicorn app:app으로 실행합니다.
# ==================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "10000")),
    )
