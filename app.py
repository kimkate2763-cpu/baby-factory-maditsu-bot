
import os
import re
import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from threading import Lock

import psycopg
from flask import Flask, request, abort, jsonify
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent


# --------------------------------------------------
# 기본 설정
# --------------------------------------------------

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("baby-factory-maditsu")

KST = ZoneInfo("Asia/Seoul")

CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not CHANNEL_SECRET:
    logger.warning("LINE_CHANNEL_SECRET 환경변수가 설정되지 않았습니다.")

if not CHANNEL_ACCESS_TOKEN:
    logger.warning("LINE_CHANNEL_ACCESS_TOKEN 환경변수가 설정되지 않았습니다.")

if not DATABASE_URL:
    logger.warning("DATABASE_URL 환경변수가 설정되지 않았습니다.")

configuration = Configuration(
    access_token=CHANNEL_ACCESS_TOKEN
)

handler = WebhookHandler(CHANNEL_SECRET)

database_lock = Lock()
profile_cache = {}
profile_lock = Lock()


# --------------------------------------------------
# 데이터베이스
# --------------------------------------------------

def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL이 설정되지 않았습니다.")

    return psycopg.connect(DATABASE_URL)


def init_database():
    with database_lock:
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS message_events (
                    event_id TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    day_kst DATE NOT NULL,
                    char_count INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS
                idx_message_events_chat_day
                ON message_events (chat_id, day_kst)
            """)

            conn.execute("""
                CREATE INDEX IF NOT EXISTS
                idx_message_events_chat_user_day
                ON message_events (chat_id, user_id, day_kst)
            """)

    logger.info("데이터베이스 초기화 완료")


# --------------------------------------------------
# 사용자 이름 가져오기
# --------------------------------------------------

def get_display_name(chat_type, chat_id, user_id):
    if not user_id:
        return "알 수 없는 사용자"

    cache_key = (chat_type, chat_id, user_id)

    with profile_lock:
        cached_name = profile_cache.get(cache_key)

    if cached_name:
        return cached_name

    display_name = "사용자-" + user_id[-6:]

    try:
        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)

            if chat_type == "group":
                profile = api.get_group_member_profile(
                    chat_id, user_id
                )
            elif chat_type == "room":
                profile = api.get_room_member_profile(
                    chat_id, user_id
                )
            else:
                profile = api.get_profile(user_id)

            if profile and profile.display_name:
                display_name = profile.display_name

    except Exception:
        logger.exception("사용자 이름 조회 실패")

    with profile_lock:
        profile_cache[cache_key] = display_name

    return display_name


# --------------------------------------------------
# 메시지 기록
# --------------------------------------------------

def record_message(event, chat_type, chat_id, user_id, text):
    # 공백, 줄바꿈, 탭 등을 제외한 글자 수
    char_count = len(re.sub(r"\s+", "", text))

    # LINE 메시지의 발송 시각을 한국 시간으로 변환
    if event.timestamp:
        message_time = datetime.fromtimestamp(
            event.timestamp / 1000,
            tz=KST
        )
    else:
        message_time = datetime.now(KST)

    message_day = message_time.date()

    display_name = get_display_name(
        chat_type, chat_id, user_id
    )

    with get_db() as conn:
        result = conn.execute(
            """
            INSERT INTO message_events (
                event_id,
                chat_id,
                user_id,
                display_name,
                day_kst,
                char_count,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (
                event.message.id,
                chat_id,
                user_id,
                display_name,
                message_day,
                char_count,
                message_time,
            ),
        )

        inserted = result.fetchone() is not None

    return inserted, message_day


# --------------------------------------------------
# 순위 조회
# --------------------------------------------------

def get_ranking(chat_id, start_day, end_day):
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT
                user_id,
                MAX(display_name) AS display_name,
                COUNT(*) AS message_count,
                COALESCE(SUM(char_count), 0) AS char_count
            FROM message_events
            WHERE chat_id = %s
              AND day_kst >= %s
              AND day_kst <= %s
            GROUP BY user_id
            ORDER BY message_count DESC,
                     char_count DESC,
                     display_name ASC
            """,
            (chat_id, start_day, end_day),
        ).fetchall()

    return rows


def format_ranking(rows, title, limit=30):
    if not rows:
        return f"{title}\n아직 기록된 마딧수가 없어."

    lines = [title, ""]

    for index, row in enumerate(rows[:limit], start=1):
        _, name, message_count, char_count = row

        lines.append(
            f"{index}위. {name}\n"
            f"마딧수: {message_count:,} | "
            f"공백 제외 글자 수: {char_count:,}"
        )

    if len(rows) > limit:
        lines.append("")
        lines.append(
            f"전체 {len(rows)}명 중 상위 {limit}명만 표시했어."
        )

    return "\n".join(lines)


def get_my_stats(chat_id, user_id, today):
    rows = get_ranking(chat_id, today, today)

    my_row = None
    my_rank = None

    for index, row in enumerate(rows, start=1):
        if row[0] == user_id:
            my_row = row
            my_rank = index
            break

    if my_row is None:
        return (
            f"내 마딧수 ({today.isoformat()})\n\n"
            "오늘 기록된 메시지가 없어.\n"
            "그룹 채팅에서 메시지를 보내면 기록이 시작돼."
        )

    _, name, message_count, char_count = my_row

    return (
        f"내 마딧수 ({today.isoformat()})\n\n"
        f"이름: {name}\n"
        f"오늘 마딧수: {message_count:,}\n"
        f"공백 제외 글자 수: {char_count:,}\n"
        f"오늘 순위: {my_rank}위 / {len(rows)}명"
    )


# --------------------------------------------------
# 명령어 처리
# --------------------------------------------------

DATE_RANGE_PATTERN = re.compile(
    r"^\*(\d{4}-\d{2}-\d{2})"
    r"\s*~\s*"
    r"(\d{4}-\d{2}-\d{2})\s*마딧수$"
)


def make_reply(text, chat_id, user_id):
    today = datetime.now(KST).date()
    command = text.strip()

    if command == "내 마딧수":
        return get_my_stats(chat_id, user_id, today)

    if command == "*마딧수":
        rows = get_ranking(chat_id, today, today)

        return format_ranking(
            rows,
            f"오늘의 마딧수 순위 ({today.isoformat()})"
        )

    match = DATE_RANGE_PATTERN.fullmatch(command)

    if match:
        try:
            start_day = date.fromisoformat(match.group(1))
            end_day = date.fromisoformat(match.group(2))
        except ValueError:
            return (
                "날짜를 올바르게 입력해 줘.\n"
                "예: *2026-11-01~2026-11-02 마딧수"
            )

        if start_day > end_day:
            return "시작 날짜는 종료 날짜보다 늦을 수 없어."

        if (end_day - start_day).days > 365:
            return "한 번에 조회할 수 있는 기간은 최대 366일이야."

        rows = get_ranking(chat_id, start_day, end_day)

        return format_ranking(
            rows,
            f"기간별 마딧수 순위\n"
            f"{start_day.isoformat()} ~ {end_day.isoformat()}"
        )

    return None


# --------------------------------------------------
# LINE 웹훅
# --------------------------------------------------

@handler.add(MessageEvent, message=TextMessageContent)
def handle_text_message(event):
    try:
        source = event.source
        text = event.message.text or ""

        # 그룹 및 다인 채팅만 집계
        if source.type == "group":
            chat_type = "group"
            chat_id = source.group_id

        elif source.type == "room":
            chat_type = "room"
            chat_id = source.room_id

        else:
            # 개인 채팅은 집계하지 않음
            return

        user_id = source.user_id

        if not user_id or not chat_id:
            return

        # 텍스트 메시지를 한 번 기록
        inserted, _ = record_message(
            event,
            chat_type,
            chat_id,
            user_id,
            text
        )

        # 중복 전달된 메시지는 재집계하거나 재응답하지 않음
        if not inserted:
            return

        # 일반 대화에는 답변하지 않고 지정 명령어에만 응답
        reply_text = make_reply(text, chat_id, user_id)

        if not reply_text:
            return

        # LINE 메시지 길이 제한에 대비
        reply_text = reply_text[:4500]

        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)

            api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[
                        TextMessage(text=reply_text)
                    ]
                )
            )

    except Exception:
        logger.exception("메시지 처리 중 오류 발생")


@app.route("/", methods=["GET"])
def home():
    return "Baby Factory Maditsu Bot is running!"


@app.route("/health", methods=["GET"])
def health():
    try:
        with get_db() as conn:
            conn.execute("SELECT 1")

        return jsonify({
            "status": "ok",
            "database": "connected"
        }), 200

    except Exception:
        logger.exception("헬스 체크 실패")

        return jsonify({
            "status": "error",
            "database": "disconnected"
        }), 503


@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")

    if not signature:
        abort(400)

    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        logger.warning("LINE 웹훅 서명 검증 실패")
        abort(400)
    except Exception:
        logger.exception("웹훅 처리 오류")
        abort(500)

    return "OK", 200


# --------------------------------------------------
# 서버 시작
# --------------------------------------------------

if __name__ == "__main__":
    init_database()

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
