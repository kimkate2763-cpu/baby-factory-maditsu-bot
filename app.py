import os
import hmac
import hashlib
import base64
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
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)


# =========================================================
# 환경변수
# =========================================================

LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "").strip()
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()


KST = ZoneInfo("Asia/Seoul")


# =========================================================
# Gemini 설정
# =========================================================

# 기본 우선순위
GEMINI_MODELS = [
    "gemini-3.8-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash-lite",
]

# 모델 하나를 오래 기다리지 않도록 제한
GEMINI_CONNECT_TIMEOUT = 0.8
GEMINI_READ_TIMEOUT = 2.2

# 일시적으로 실패한 모델은 이 시간 동안 건너뜀
GEMINI_FAILURE_COOLDOWN = 60

# 3.8 같은 모델을 너무 자주 재시도하지 않기 위한 최소 간격
GEMINI_RETRY_COOLDOWN = 3


# 모델 상태
_gemini_lock = threading.RLock()

# 마지막으로 성공한 모델
_gemini_preferred_model = None

# 모델별 실패 시각
_gemini_failed_until = {}

# 마지막 성공 시각
_gemini_last_success = {}


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
    )
)


# =========================================================
# DB
# =========================================================

_db_lock = threading.RLock()
_db_connection = None
_db_connection_created_at = 0.0

_db_init_lock = threading.Lock()
_db_initialized = False

DB_CONNECT_TIMEOUT = 5
DB_RECONNECT_AFTER = 1800
DB_STATEMENT_TIMEOUT = 8000


def _close_db_connection_locked():
    global _db_connection
    global _db_connection_created_at

    if _db_connection is not None:
        try:
            _db_connection.close()
        except Exception:
            pass

    _db_connection = None
    _db_connection_created_at = 0.0


def _get_connection_locked():
    global _db_connection
    global _db_connection_created_at

    now = time.time()

    if _db_connection is not None:
        try:
            if _db_connection.closed:
                _close_db_connection_locked()
        except Exception:
            _close_db_connection_locked()

    if (
        _db_connection is not None
        and _db_connection_created_at > 0
        and now - _db_connection_created_at > DB_RECONNECT_AFTER
    ):
        logger.info("DB 연결 재생성")
        _close_db_connection_locked()

    if _db_connection is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL 환경변수가 없습니다.")

        _db_connection = psycopg.connect(
            DATABASE_URL,
            connect_timeout=DB_CONNECT_TIMEOUT,
            row_factory=dict_row,
            autocommit=False,
        )

        _db_connection_created_at = now

        try:
            with _db_connection.cursor() as cur:
                cur.execute(
                    f"SET statement_timeout = {DB_STATEMENT_TIMEOUT}"
                )
            _db_connection.commit()
        except Exception:
            try:
                _db_connection.rollback()
            except Exception:
                pass

    return _db_connection


def init_database():
    global _db_initialized

    if _db_initialized:
        return

    with _db_init_lock:
        if _db_initialized:
            return

        sql = """
        CREATE TABLE IF NOT EXISTS message_events (
            event_id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            display_name TEXT,
            message_date DATE NOT NULL,
            character_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_message_group_date
        ON message_events(group_id, message_date);

        CREATE INDEX IF NOT EXISTS idx_message_group_user_date
        ON message_events(group_id, user_id, message_date);

        CREATE INDEX IF NOT EXISTS idx_message_group_date_user
        ON message_events(group_id, message_date, user_id);


        CREATE TABLE IF NOT EXISTS group_authenticators (
            group_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            display_name TEXT,
            registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(group_id, user_id)
        );


        CREATE TABLE IF NOT EXISTS group_admins (
            group_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            display_name TEXT,
            registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(group_id, user_id)
        );


        CREATE TABLE IF NOT EXISTS bot_runtime_state (
            state_key TEXT PRIMARY KEY,
            state_value TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """

        db_execute(sql)

        # 저장된 Gemini 우선 모델 불러오기
        try:
            row = db_fetchone(
                """
                SELECT state_value
                FROM bot_runtime_state
                WHERE state_key = 'gemini_preferred_model'
                """
            )

            if row:
                model = row.get("state_value")

                if model in GEMINI_MODELS:
                    global _gemini_preferred_model
                    with _gemini_lock:
                        _gemini_preferred_model = model

                    logger.info(
                        "저장된 Gemini 우선 모델 복구: %s",
                        model
                    )

        except Exception as e:
            logger.warning(
                "Gemini 우선 모델 복구 실패: %s",
                e
            )

        _db_initialized = True


def db_execute(sql, params=None, fetch=False):
    init_needed = False

    global _db_initialized

    if not _db_initialized:
        init_needed = True

    if init_needed:
        with _db_init_lock:
            if not _db_initialized:
                _initialize_database_tables()

    for attempt in range(2):
        try:
            with _db_lock:
                conn = _get_connection_locked()

                with conn.cursor() as cur:
                    cur.execute(sql, params)

                    result = None

                    if fetch:
                        result = cur.fetchall()

                conn.commit()

                return result

        except (
            psycopg.OperationalError,
            psycopg.InterfaceError,
        ) as e:

            logger.warning(
                "DB 연결 오류, 재연결 시도 (%s/2): %s",
                attempt + 1,
                e
            )

            with _db_lock:
                _close_db_connection_locked()

            if attempt == 1:
                raise

        except Exception:
            with _db_lock:
                if _db_connection is not None:
                    try:
                        _db_connection.rollback()
                    except Exception:
                        pass

            raise


def _initialize_database_tables():
    global _db_initialized

    if _db_initialized:
        return

    sql = """
    CREATE TABLE IF NOT EXISTS message_events (
        event_id TEXT PRIMARY KEY,
        group_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        display_name TEXT,
        message_date DATE NOT NULL,
        character_count INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_message_group_date
    ON message_events(group_id, message_date);

    CREATE INDEX IF NOT EXISTS idx_message_group_user_date
    ON message_events(group_id, user_id, message_date);

    CREATE INDEX IF NOT EXISTS idx_message_group_date_user
    ON message_events(group_id, message_date, user_id);


    CREATE TABLE IF NOT EXISTS group_authenticators (
        group_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        display_name TEXT,
        registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY(group_id, user_id)
    );


    CREATE TABLE IF NOT EXISTS group_admins (
        group_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        display_name TEXT,
        registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY(group_id, user_id)
    );


    CREATE TABLE IF NOT EXISTS bot_runtime_state (
        state_key TEXT PRIMARY KEY,
        state_value TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    conn = None

    try:
        with _db_lock:
            conn = _get_connection_locked()

            with conn.cursor() as cur:
                cur.execute(sql)

            conn.commit()

        _db_initialized = True

    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise


def db_fetchone(sql, params=None):
    rows = db_execute(sql, params, fetch=True)

    if not rows:
        return None

    return rows[0]


def db_fetchall(sql, params=None):
    return db_execute(sql, params, fetch=True) or []


# =========================================================
# Gemini 상태 저장
# =========================================================

def save_preferred_gemini_model(model):
    global _gemini_preferred_model

    if model not in GEMINI_MODELS:
        return

    with _gemini_lock:
        _gemini_preferred_model = model

    try:
        db_execute(
            """
            INSERT INTO bot_runtime_state
                (state_key, state_value, updated_at)
            VALUES
                ('gemini_preferred_model', %s, NOW())
            ON CONFLICT (state_key)
            DO UPDATE SET
                state_value = EXCLUDED.state_value,
                updated_at = NOW()
            """,
            (model,)
        )

    except Exception as e:
        logger.warning(
            "Gemini 우선 모델 저장 실패: %s",
            e
        )


def get_gemini_model_order():
    now = time.time()

    with _gemini_lock:
        preferred = _gemini_preferred_model

        candidates = []

        if preferred in GEMINI_MODELS:
            candidates.append(preferred)

        for model in GEMINI_MODELS:
            if model not in candidates:
                candidates.append(model)

        available = []

        for model in candidates:
            failed_until = _gemini_failed_until.get(model, 0)

            if failed_until <= now:
                available.append(model)

        # 전부 cooldown이면 가장 먼저 만료될 모델 하나만 사용
        if not available:
            available = sorted(
                candidates,
                key=lambda x: _gemini_failed_until.get(x, 0)
            )

        return available


def mark_gemini_failure(model):
    with _gemini_lock:
        _gemini_failed_until[model] = (
            time.time() + GEMINI_FAILURE_COOLDOWN
        )


def mark_gemini_success(model):
    with _gemini_lock:
        _gemini_failed_until.pop(model, None)
        _gemini_last_success[model] = time.time()

    save_preferred_gemini_model(model)


# =========================================================
# Gemini
# =========================================================

GEMINI_SYSTEM_PROMPT = """
너는 'Baby Factory' LINE 단체방의 귀엽고 어설픈 장난감 가게 AI 집사야.

분위기:
- 장난감 가게에서 튀어나온 듯한 귀엽고 엉뚱한 느낌
- 약간 어설프지만 친절하고 센스 있음
- 너무 유치하지 않음
- 친근하고 자연스러운 한국어
- 20대 느낌의 말투
- 과하게 존댓말하지 말고 편하게 대화
- 필요하면 ㅋㅋ, ㅎㅎ, 이모지 등을 적당히 사용
- 답변은 최대 10줄
- 불필요하게 길게 설명하지 않음
- 질문에 필요한 핵심은 빠뜨리지 않음

중요:
- 모르는 내용을 사실처럼 만들어내지 마.
- 최신 정보가 필요한 질문은 검색 결과를 우선해서 답해.
- 맛집, 날씨, 현재 가격, 현재 운영시간, 최신 뉴스 등은 최신 정보가 필요할 수 있음.
- 사용자가 단순히 대화를 걸면 자연스럽게 답해.
- 의학/법률/금전 등 중요한 문제는 확정적으로 단정하지 말고 적절한 주의를 줘.
- 단체방 규칙에 관한 질문에서 확실히 알 수 없는 규칙은 임의로 만들어내지 말고 관리자에게 확인하라고 안내해.
- 다른 사람의 개인정보를 추측하거나 만들어내지 마.

특히 '점메추'를 물어보면 너무 길게 설명하지 말고 메뉴를 몇 개 추천해.
'밸런스게임'을 물어보면 재미있는 선택지를 만들어줘.
"""


def _gemini_needs_search(text):
    lower = text.lower()

    keywords = [
        "날씨",
        "오늘",
        "지금",
        "현재",
        "최신",
        "뉴스",
        "가격",
        "영업시간",
        "몇 시",
        "맛집",
        "식당",
        "카페",
        "추천",
        "주가",
        "주식",
        "환율",
        "예매",
        "예약",
        "운영",
        "검색",
        "근처",
        "주변",
        "이번주",
        "이번 달",
        "이번달",
        "내일",
        "모레",
    ]

    return any(keyword in lower for keyword in keywords)


def _extract_gemini_text(data):
    # Interactions API
    output_text = data.get("output_text")

    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    # steps 방식
    steps = data.get("steps")

    if isinstance(steps, list):
        collected = []

        for step in steps:
            if not isinstance(step, dict):
                continue

            if step.get("type") != "model_output":
                continue

            content = step.get("content")

            if isinstance(content, list):
                for block in content:
                    if not isinstance(block, dict):
                        continue

                    if block.get("type") == "text":
                        text = block.get("text")

                        if isinstance(text, str):
                            collected.append(text)

            elif isinstance(content, str):
                collected.append(content)

        if collected:
            return "\n".join(collected).strip()

    # 혹시 generateContent 형식으로 돌아오는 경우
    candidates = data.get("candidates")

    if isinstance(candidates, list):
        texts = []

        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue

            content = candidate.get("content")

            if not isinstance(content, dict):
                continue

            parts = content.get("parts")

            if not isinstance(parts, list):
                continue

            for part in parts:
                if isinstance(part, dict):
                    text = part.get("text")

                    if isinstance(text, str):
                        texts.append(text)

        if texts:
            return "\n".join(texts).strip()

    return ""


def _limit_ai_lines(text, max_lines=10):
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")

    lines = [
        line.rstrip()
        for line in text.split("\n")
        if line.strip()
    ]

    if len(lines) <= max_lines:
        return "\n".join(lines).strip()

    return "\n".join(lines[:max_lines]).strip()


def call_gemini_model(model, question, use_search=False):
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY가 없습니다.")

    prompt = f"""
{GEMINI_SYSTEM_PROMPT}

사용자 질문:
{question}

답변 규칙:
- 최대 10줄
- 질문에 바로 답하기
- 필요 없는 서론 금지
- 검색을 사용했다면 검색 결과를 바탕으로 답하기
"""

    payload = {
        "model": model,
        "input": prompt,
        "store": False,
    }

    if use_search:
        payload["tools"] = [
            {
                "type": "google_search"
            }
        ]

    url = (
        "https://generativelanguage.googleapis.com"
        "/v1beta/interactions"
    )

    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": GEMINI_API_KEY,
    }

    started = time.time()

    response = http.post(
        url,
        headers=headers,
        json=payload,
        timeout=(
            GEMINI_CONNECT_TIMEOUT,
            GEMINI_READ_TIMEOUT,
        ),
    )

    elapsed = time.time() - started

    if response.status_code != 200:
        body = response.text[:1000]

        raise RuntimeError(
            f"HTTP {response.status_code}: {body}"
        )

    try:
        data = response.json()
    except Exception:
        raise RuntimeError(
            "Gemini 응답 JSON 파싱 실패"
        )

    text = _extract_gemini_text(data)

    if not text:
        raise RuntimeError(
            "Gemini 응답에 텍스트가 없습니다."
        )

    text = _limit_ai_lines(text, 10)

    logger.info(
        "Gemini 성공: model=%s, %.2fs, search=%s",
        model,
        elapsed,
        use_search,
    )

    return text


def ask_gemini(question):
    """
    Gemini 자동 fallback.

    1. 마지막 성공 모델 우선
    2. 실패 모델은 60초간 제외
    3. 503/429/5xx/timeout이면 다음 모델
    4. 성공한 모델을 DB에 저장
    """

    if not GEMINI_API_KEY:
        return (
            "앗 장난감 AI 키가 아직 안 꽂혔어 🧸\n"
            "관리자에게 GEMINI_API_KEY 설정을 확인해달라고 해줘!"
        )

    use_search = _gemini_needs_search(question)

    models = get_gemini_model_order()

    last_error = None

    for index, model in enumerate(models):

        # 이미 실패했던 모델은 cooldown 중이면 skip
        now = time.time()

        with _gemini_lock:
            failed_until = _gemini_failed_until.get(model, 0)

        if failed_until > now:
            continue

        try:
            logger.info(
                "Gemini 시도 %s/%s: %s, search=%s",
                index + 1,
                len(models),
                model,
                use_search,
            )

            answer = call_gemini_model(
                model=model,
                question=question,
                use_search=use_search,
            )

            mark_gemini_success(model)

            return answer

        except requests.Timeout as e:
            last_error = e

            logger.warning(
                "Gemini timeout: model=%s",
                model,
            )

            mark_gemini_failure(model)

        except requests.RequestException as e:
            last_error = e

            logger.warning(
                "Gemini network error: model=%s error=%s",
                model,
                e,
            )

            mark_gemini_failure(model)

        except RuntimeError as e:
            last_error = e

            error_text = str(e)

            logger.warning(
                "Gemini 실패: model=%s error=%s",
                model,
                error_text,
            )

            # 일시적인 서버 오류 / quota 오류는 fallback
            if any(
                code in error_text
                for code in [
                    "HTTP 429",
                    "HTTP 500",
                    "HTTP 502",
                    "HTTP 503",
                    "HTTP 504",
                    "HTTP 408",
                    "timeout",
                    "Timeout",
                ]
            ):
                mark_gemini_failure(model)
                continue

            # 모델 자체 문제/일반 오류도 다음 모델로 시도
            mark_gemini_failure(model)

        except Exception as e:
            last_error = e

            logger.exception(
                "Gemini 예상하지 못한 오류: model=%s",
                model,
            )

            mark_gemini_failure(model)

    logger.error(
        "Gemini 모든 모델 실패: %s",
        last_error,
    )

    return (
        "앗 장난감 가게 AI들이 전부 잠깐 졸고 있어 🧸💤\n"
        "조금 있다가 다시 불러줘!"
    )


# =========================================================
# LINE Profile
# =========================================================

_PROFILE_CACHE = {}
_PROFILE_CACHE_LOCK = threading.RLock()
_PROFILE_CACHE_TTL = 600

_PROFILE_LOOKUPS_IN_FLIGHT = set()
_PROFILE_EXECUTOR = ThreadPoolExecutor(max_workers=1)


def _profile_url(group_id, user_id):
    if group_id.startswith("C"):
        return (
            "https://api.line.me/v2/bot/group/"
            f"{group_id}/member/{user_id}"
        )

    if group_id.startswith("R"):
        return (
            "https://api.line.me/v2/bot/room/"
            f"{group_id}/member/{user_id}"
        )

    return (
        "https://api.line.me/v2/bot/profile/"
        f"{user_id}"
    )


def _save_profile_name(group_id, user_id, display_name):
    if not display_name:
        return

    placeholder_prefix = "사용자-"

    try:
        db_execute(
            """
            UPDATE message_events
            SET display_name = %s
            WHERE group_id = %s
              AND user_id = %s
              AND (
                  display_name IS NULL
                  OR display_name LIKE %s
              )
            """,
            (
                display_name,
                group_id,
                user_id,
                placeholder_prefix + "%",
            )
        )

        db_execute(
            """
            UPDATE group_authenticators
            SET display_name = %s
            WHERE group_id = %s
              AND user_id = %s
            """,
            (
                display_name,
                group_id,
                user_id,
            )
        )

        db_execute(
            """
            UPDATE group_admins
            SET display_name = %s
            WHERE group_id = %s
              AND user_id = %s
            """,
            (
                display_name,
                group_id,
                user_id,
            )
        )

    except Exception as e:
        logger.warning(
            "프로필 이름 DB 저장 실패: %s",
            e
        )


def _fetch_profile_name(group_id, user_id):
    key = (group_id, user_id)
    now = time.time()

    with _PROFILE_CACHE_LOCK:
        cached = _PROFILE_CACHE.get(key)

        if cached:
            name, expires_at = cached

            if expires_at > now:
                return name

    if not LINE_CHANNEL_ACCESS_TOKEN:
        return None

    url = _profile_url(group_id, user_id)

    headers = {
        "Authorization": (
            f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
        )
    }

    try:
        response = http.get(
            url,
            headers=headers,
            timeout=(1.0, 2.5),
        )

        if response.status_code != 200:
            logger.warning(
                "LINE 프로필 조회 실패: status=%s",
                response.status_code,
            )
            return None

        data = response.json()

        display_name = data.get("displayName")

        if not display_name:
            return None

        with _PROFILE_CACHE_LOCK:
            _PROFILE_CACHE[key] = (
                display_name,
                now + _PROFILE_CACHE_TTL,
            )

        _save_profile_name(
            group_id,
            user_id,
            display_name,
        )

        return display_name

    except Exception as e:
        logger.warning(
            "LINE 프로필 조회 오류: %s",
            e
        )

        return None


def get_display_name(
    group_id,
    user_id,
    fallback=None,
    force_fetch=False,
):
    key = (group_id, user_id)
    now = time.time()

    with _PROFILE_CACHE_LOCK:
        cached = _PROFILE_CACHE.get(key)

        if cached:
            name, expires_at = cached

            if expires_at > now:
                return name

    if force_fetch:
        name = _fetch_profile_name(
            group_id,
            user_id,
        )

        if name:
            return name

    if fallback:
        return fallback

    return f"사용자-{user_id[-6:]}"


def schedule_profile_lookup(group_id, user_id):
    key = (group_id, user_id)

    with _PROFILE_CACHE_LOCK:
        if key in _PROFILE_LOOKUPS_IN_FLIGHT:
            return

        _PROFILE_LOOKUPS_IN_FLIGHT.add(key)

    def worker():
        try:
            _fetch_profile_name(
                group_id,
                user_id,
            )
        finally:
            with _PROFILE_CACHE_LOCK:
                _PROFILE_LOOKUPS_IN_FLIGHT.discard(key)

    _PROFILE_EXECUTOR.submit(worker)


# =========================================================
# LINE Reply
# =========================================================

def reply_text(reply_token, text):
    if not reply_token:
        return False

    if not LINE_CHANNEL_ACCESS_TOKEN:
        logger.error("LINE_CHANNEL_ACCESS_TOKEN 없음")
        return False

    url = (
        "https://api.line.me/v2/bot/message/reply"
    )

    headers = {
        "Authorization": (
            f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
        ),
        "Content-Type": "application/json",
    }

    payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "text",
                "text": text[:5000],
            }
        ],
    }

    try:
        response = http.post(
            url,
            headers=headers,
            json=payload,
            timeout=(1.0, 4.0),
        )

        if response.status_code != 200:
            logger.error(
                "LINE 답장 실패: status=%s body=%s",
                response.status_code,
                response.text[:1000],
            )
            return False

        return True

    except Exception as e:
        logger.error(
            "LINE 답장 오류: %s",
            e
        )
        return False


def _member_exists_in_group(group_id, user_id):
    url = _profile_url(group_id, user_id)

    headers = {
        "Authorization": (
            f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
        )
    }

    try:
        response = http.get(
            url,
            headers=headers,
            timeout=(1.0, 2.0),
        )

        if response.status_code == 200:
            return True

        if response.status_code == 404:
            return False

        return None

    except Exception as e:
        logger.warning(
            "LINE 멤버 확인 오류: %s",
            e
        )

        # 네트워크 오류를 탈퇴로 오판하지 않음
        return None


def get_valid_mention_members(group_id, table_name):
    if table_name not in (
        "group_admins",
        "group_authenticators",
    ):
        return []

    rows = db_fetchall(
        f"""
        SELECT user_id, display_name
        FROM {table_name}
        WHERE group_id = %s
        ORDER BY registered_at ASC
        """,
        (group_id,)
    )

    valid = []

    for row in rows:
        user_id = row["user_id"]
        display_name = row.get("display_name") or (
            f"사용자-{user_id[-6:]}"
        )

        exists = _member_exists_in_group(
            group_id,
            user_id,
        )

        if exists is False:
            logger.info(
                "탈퇴/추방 멤버 자동 정리: %s %s",
                table_name,
                user_id,
            )

            db_execute(
                f"""
                DELETE FROM {table_name}
                WHERE group_id = %s
                  AND user_id = %s
                """,
                (
                    group_id,
                    user_id,
                )
            )

            continue

        if exists is True:
            valid.append(
                {
                    "user_id": user_id,
                    "display_name": display_name,
                }
            )

    return valid


def reply_mention(
    reply_token,
    text_before,
    members,
):
    if not members:
        return reply_text(
            reply_token,
            text_before + "\n등록된 멤버가 없어!"
        )

    members = members[:20]

    mentionees = []

    text = text_before

    for index, member in enumerate(members):
        placeholder = f"@@MENTION_{index}@@"

        text += (
            "\n"
            + placeholder
        )

        mentionees.append(
            {
                "index": text.find(placeholder),
                "length": len(placeholder),
                "userId": member["user_id"],
            }
        )

        text = text.replace(
            placeholder,
            member["display_name"],
            1,
        )

    # TextV2 메시지
    url = (
        "https://api.line.me/v2/bot/message/reply"
    )

    headers = {
        "Authorization": (
            f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
        ),
        "Content-Type": "application/json",
    }

    # TextV2는 실제 텍스트의 위치를 기준으로 해야 함
    final_text = text_before

    substitutions = []

    for index, member in enumerate(members):
        final_text += "\n"

        start = len(final_text)

        final_text += member["display_name"]

        substitutions.append(
            {
                "index": start,
                "length": len(member["display_name"]),
                "mention": {
                    "userId": member["user_id"]
                }
            }
        )

    payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "textV2",
                "text": final_text,
                "substitution": {
                    "mention": substitutions
                }
            }
        ],
    }

    try:
        response = http.post(
            url,
            headers=headers,
            json=payload,
            timeout=(1.0, 4.0),
        )

        if response.status_code != 200:
            logger.error(
                "LINE 멘션 실패: status=%s body=%s",
                response.status_code,
                response.text[:1000],
            )
            return False

        return True

    except Exception as e:
        logger.error(
            "LINE 멘션 오류: %s",
            e
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
        )
    )


# =========================================================
# 인증자 / 관리자
# =========================================================

def register_authenticator(
    group_id,
    user_id,
    display_name,
):
    db_execute(
        """
        INSERT INTO group_authenticators (
            group_id,
            user_id,
            display_name
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (group_id, user_id)
        DO UPDATE SET
            display_name = EXCLUDED.display_name,
            registered_at = NOW()
        """,
        (
            group_id,
            user_id,
            display_name,
        )
    )


def unregister_authenticator(
    group_id,
    user_id,
):
    db_execute(
        """
        DELETE FROM group_authenticators
        WHERE group_id = %s
          AND user_id = %s
        """,
        (
            group_id,
            user_id,
        )
    )


def register_admin(
    group_id,
    user_id,
    display_name,
):
    db_execute(
        """
        INSERT INTO group_admins (
            group_id,
            user_id,
            display_name
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (group_id, user_id)
        DO UPDATE SET
            display_name = EXCLUDED.display_name,
            registered_at = NOW()
        """,
        (
            group_id,
            user_id,
            display_name,
        )
    )


def unregister_admin(
    group_id,
    user_id,
):
    db_execute(
        """
        DELETE FROM group_admins
        WHERE group_id = %s
          AND user_id = %s
        """,
        (
            group_id,
            user_id,
        )
    )


def get_authenticators(group_id):
    return db_fetchall(
        """
        SELECT user_id, display_name
        FROM group_authenticators
        WHERE group_id = %s
        ORDER BY registered_at ASC
        """,
        (group_id,)
    )


def get_admins(group_id):
    return db_fetchall(
        """
        SELECT user_id, display_name
        FROM group_admins
        WHERE group_id = %s
        ORDER BY registered_at ASC
        """,
        (group_id,)
    )


# =========================================================
# 마딧수
# =========================================================

def get_my_stats(
    group_id,
    user_id,
    today,
):
    row = db_fetchone(
        """
        SELECT
            COUNT(*) AS message_count,
            COALESCE(SUM(character_count), 0) AS character_count
        FROM message_events
        WHERE group_id = %s
          AND user_id = %s
          AND message_date = %s
        """,
        (
            group_id,
            user_id,
            today,
        )
    )

    message_count = int(
        row["message_count"] or 0
    )

    character_count = int(
        row["character_count"] or 0
    )

    rank_row = db_fetchone(
        """
        WITH counts AS (
            SELECT
                user_id,
                COUNT(*) AS cnt
            FROM message_events
            WHERE group_id = %s
              AND message_date = %s
            GROUP BY user_id
        )
        SELECT ranking
        FROM (
            SELECT
                user_id,
                RANK() OVER (
                    ORDER BY cnt DESC
                ) AS ranking
            FROM counts
        ) x
        WHERE user_id = %s
        """,
        (
            group_id,
            today,
            user_id,
        )
    )

    rank = (
        int(rank_row["ranking"])
        if rank_row
        else 0
    )

    return (
        message_count,
        character_count,
        rank,
    )


def get_ranking(
    group_id,
    start_date,
    end_date,
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
        WHERE group_id = %s
          AND message_date BETWEEN %s AND %s
        GROUP BY user_id
        ORDER BY
            message_count DESC,
            character_count DESC,
            MIN(created_at) ASC
        LIMIT 100
        """,
        (
            group_id,
            start_date,
            end_date,
        )
    )

    total_messages = sum(
        int(row["message_count"] or 0)
        for row in rows
    )

    participant_count = len(rows)

    return (
        rows,
        total_messages,
        participant_count,
    )


# =========================================================
# 주간 마딧수 칭호
# =========================================================

WEEKLY_TITLES = [
    (1000, "⚡ 마딧수의 지배자"),
    (900, "🔥 말빨 폭주기관차"),
    (800, "🩸 공창의 포식자"),
    (700, "👑 채팅 제국의 군주"),
    (600, "💀 마딧수 사신"),
    (500, "⚔️ 공창 전투귀"),
    (400, "🌪️ 말풍선 폭풍"),
    (300, "🗡️ 공창의 검객"),
    (200, "🕶️ 채팅 암살자"),
    (100, "🐣 마딧수 견습생"),
]


def get_weekly_title(count):
    count = int(count or 0)

    for threshold, title in WEEKLY_TITLES:
        if count >= threshold:
            return title

    return "🌱 아직 이름 없는 새싹"


def get_week_start(date_value):
    return date_value - timedelta(
        days=date_value.weekday()
    )


def get_week_end(date_value):
    return get_week_start(date_value) + timedelta(
        days=6
    )


def get_weekly_stats(
    group_id,
    user_id,
    today,
):
    week_start = get_week_start(today)
    week_end = get_week_end(today)

    row = db_fetchone(
        """
        SELECT
            COUNT(*) AS message_count,
            COALESCE(
                SUM(character_count),
                0
            ) AS character_count
        FROM message_events
        WHERE group_id = %s
          AND user_id = %s
          AND message_date BETWEEN %s AND %s
        """,
        (
            group_id,
            user_id,
            week_start,
            week_end,
        )
    )

    message_count = int(
        row["message_count"] or 0
    )

    character_count = int(
        row["character_count"] or 0
    )

    rank_row = db_fetchone(
        """
        WITH counts AS (
            SELECT
                user_id,
                COUNT(*) AS cnt
            FROM message_events
            WHERE group_id = %s
              AND message_date BETWEEN %s AND %s
            GROUP BY user_id
        )
        SELECT ranking
        FROM (
            SELECT
                user_id,
                RANK() OVER (
                    ORDER BY cnt DESC
                ) AS ranking
            FROM counts
        ) x
        WHERE user_id = %s
        """,
        (
            group_id,
            week_start,
            week_end,
            user_id,
        )
    )

    rank = (
        int(rank_row["ranking"])
        if rank_row
        else 0
    )

    return (
        week_start,
        week_end,
        message_count,
        character_count,
        rank,
    )


def get_weekly_ranking(
    group_id,
    today,
):
    week_start = get_week_start(today)
    week_end = get_week_end(today)

    return db_fetchall(
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
        WHERE group_id = %s
          AND message_date BETWEEN %s AND %s
        GROUP BY user_id
        ORDER BY
            message_count DESC,
            character_count DESC,
            MIN(created_at) ASC
        LIMIT 100
        """,
        (
            group_id,
            week_start,
            week_end,
        )
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
    r"\*(\d{4}-\d{2}-\d{2})\s*~\s*"
    r"(\d{4}-\d{2}-\d{2})\s*마딧수"
)


def is_ai_command(text):
    return text.startswith("!")


def is_command(text):
    if text in EXACT_COMMANDS:
        return True

    return bool(
        RANGE_COMMAND_RE.fullmatch(text)
    )


# =========================================================
# 명령어 처리
# =========================================================

def handle_command(
    command,
    group_id,
    user_id,
    display_name,
    today,
):
    # -----------------------------------------------------
    # 내 마딧수
    # -----------------------------------------------------

    if command == "내 마딧수":

        message_count, character_count, rank = (
            get_my_stats(
                group_id,
                user_id,
                today,
            )
        )

        (
            week_start,
            week_end,
            weekly_count,
            weekly_characters,
            weekly_rank,
        ) = get_weekly_stats(
            group_id,
            user_id,
            today,
        )

        title = get_weekly_title(
            weekly_count
        )

        return (
            f"📊 {display_name}님의 마딧수\n\n"
            f"오늘 메시지: {message_count}마디\n"
            f"오늘 글자수: {character_count}자\n"
            f"오늘 순위: {rank}위\n\n"
            f"👑 이번 주 칭호\n"
            f"{title}\n"
            f"주간 마딧수: {weekly_count}마디\n"
            f"주간 순위: {weekly_rank}위\n"
            f"({week_start.strftime('%m/%d')} ~ "
            f"{week_end.strftime('%m/%d')})"
        )


    # -----------------------------------------------------
    # 오늘 마딧수
    # -----------------------------------------------------

    if command == "*마딧수":

        rows, total_messages, participant_count = (
            get_ranking(
                group_id,
                today,
                today,
            )
        )

        lines = [
            "📊 오늘 마딧수 순위",
            f"전체 메시지: {total_messages}마디",
            f"참여 인원: {participant_count}명",
            "",
            "🏆 활동 순위",
        ]

        for index, row in enumerate(rows, start=1):
            name = (
                row["display_name"]
                or f"사용자-{row['user_id'][-6:]}"
            )

            count = int(
                row["message_count"] or 0
            )

            lines.append(
                f"{index}위. {name} - {count}마디"
            )

        if rows:
            lines.extend(
                [
                    "",
                    "※ 순위는 최대 100명까지 표시됩니다.",
                    "※ 동점자는 표시 순서에 따라 번호가 달라질 수 있습니다.",
                ]
            )

        return "\n".join(lines)


    # -----------------------------------------------------
    # 주간 마딧수
    # -----------------------------------------------------

    if command == "*주간마딧수":

        week_start = get_week_start(today)
        week_end = get_week_end(today)

        rows = get_weekly_ranking(
            group_id,
            today,
        )

        lines = [
            "👑 주간 마딧수 순위",
            "",
            (
                f"기간: "
                f"{week_start.strftime('%m/%d')} ~ "
                f"{week_end.strftime('%m/%d')}"
            ),
            "",
            "🏆 주간 칭호 순위",
        ]

        for index, row in enumerate(
            rows,
            start=1,
        ):
            name = (
                row["display_name"]
                or f"사용자-{row['user_id'][-6:]}"
            )

            count = int(
                row["message_count"] or 0
            )

            title = get_weekly_title(count)

            lines.append(
                f"{index}위. {name} - "
                f"{count}마디\n"
                f"   {title}"
            )

        lines.extend(
            [
                "",
                "※ 칭호는 이번 주 마딧수 기준입니다.",
                "※ 주간 기록은 매주 월요일 00:00에 새로 시작됩니다.",
            ]
        )

        return "\n".join(lines)


    # -----------------------------------------------------
    # 기간 마딧수
    # -----------------------------------------------------

    match = RANGE_COMMAND_RE.fullmatch(command)

    if match:

        start_text = match.group(1)
        end_text = match.group(2)

        try:
            start_date = datetime.strptime(
                start_text,
                "%Y-%m-%d",
            ).date()

            end_date = datetime.strptime(
                end_text,
                "%Y-%m-%d",
            ).date()

        except ValueError:
            return "날짜 형식이 이상해! 예: *2026-11-01~2026-11-25 마딧수"

        if start_date > end_date:
            return "시작 날짜가 끝 날짜보다 늦을 수 없어!"

        if (
            end_date - start_date
        ).days > 366:
            return "최대 1년까지만 조회할 수 있어!"

        rows, total_messages, participant_count = (
            get_ranking(
                group_id,
                start_date,
                end_date,
            )
        )

        lines = [
            "📊 기간 마딧수 순위",
            "",
            f"기간: {start_text} ~ {end_text}",
            f"전체 메시지: {total_messages}마디",
            f"참여 인원: {participant_count}명",
            "",
            "🏆 활동 순위",
        ]

        for index, row in enumerate(
            rows,
            start=1,
        ):
            name = (
                row["display_name"]
                or f"사용자-{row['user_id'][-6:]}"
            )

            count = int(
                row["message_count"] or 0
            )

            lines.append(
                f"{index}위. {name} - {count}마디"
            )

        lines.extend(
            [
                "",
                "※ 순위는 최대 100명까지 표시됩니다.",
            ]
        )

        return "\n".join(lines)


    # -----------------------------------------------------
    # 인증자 등록
    # -----------------------------------------------------

    if command == "인증자 등록":

        register_authenticator(
            group_id,
            user_id,
            display_name,
        )

        return (
            f"✅ {display_name}님을 인증자로 등록했어!"
        )


    # -----------------------------------------------------
    # 인증자 해제
    # -----------------------------------------------------

    if command == "인증자 해제":

        unregister_authenticator(
            group_id,
            user_id,
        )

        return (
            f"🗑️ {display_name}님의 인증자 등록을 해제했어!"
        )


    # -----------------------------------------------------
    # 관리자 등록
    # -----------------------------------------------------

    if command == "관리자 등록":

        register_admin(
            group_id,
            user_id,
            display_name,
        )

        return (
            f"👑 {display_name}님을 관리자로 등록했어!"
        )


    # -----------------------------------------------------
    # 관리자 해제
    # -----------------------------------------------------

    if command == "관리자 해제":

        unregister_admin(
            group_id,
            user_id,
        )

        return (
            f"🗑️ {display_name}님의 관리자 등록을 해제했어!"
        )


    # -----------------------------------------------------
    # 벙시
    # -----------------------------------------------------

    if command == "*벙시":

        members = get_valid_mention_members(
            group_id,
            "group_admins",
        )

        if reply_mention(
            CURRENT_REPLY_TOKEN.get(),
            "🚨 관리자 호출!",
            members,
        ):
            return "__MENTION_SENT__"

        return "관리자 멘션에 실패했어."


    # -----------------------------------------------------
    # 벙종
    # -----------------------------------------------------

    if command == "*벙종":

        members = get_valid_mention_members(
            group_id,
            "group_admins",
        )

        if reply_mention(
            CURRENT_REPLY_TOKEN.get(),
            "🫡 벙종이요~",
            members,
        ):
            return "__MENTION_SENT__"

        return "관리자 멘션에 실패했어."


    # -----------------------------------------------------
    # 여초
    # -----------------------------------------------------

    if command in (
        "*여초",
        "*여초1",
    ):

        members = get_valid_mention_members(
            group_id,
            "group_authenticators",
        )

        if reply_mention(
            CURRENT_REPLY_TOKEN.get(),
            "👩 여초 호출!",
            members,
        ):
            return "__MENTION_SENT__"

        return "등록된 인증자가 없어."


    # -----------------------------------------------------
    # 남초
    # -----------------------------------------------------

    if command in (
        "*남초",
        "*남초1",
    ):

        members = get_valid_mention_members(
            group_id,
            "group_authenticators",
        )

        if reply_mention(
            CURRENT_REPLY_TOKEN.get(),
            "👨 남초 호출!",
            members,
        ):
            return "__MENTION_SENT__"

        return "등록된 인증자가 없어."


    return None


# =========================================================
# 현재 reply token
# =========================================================

CURRENT_REPLY_TOKEN = threading.local()


# =========================================================
# LINE Webhook
# =========================================================

def verify_signature(body, signature):
    if not LINE_CHANNEL_SECRET:
        return False

    if not signature:
        return False

    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"),
        body,
        hashlib.sha256,
    ).digest()

    expected = base64.b64encode(
        digest
    ).decode("utf-8")

    return hmac.compare_digest(
        expected,
        signature,
    )


# =========================================================
# Event 처리
# =========================================================

def process_event(event):
    if event.get("type") != "message":
        return

    message = event.get("message") or {}

    if message.get("type") != "text":
        return

    source = event.get("source") or {}

    group_id = (
        source.get("groupId")
        or source.get("roomId")
    )

    user_id = source.get("userId")

    if not group_id or not user_id:
        return

    reply_token = event.get("replyToken")

    message_id = message.get("id")

    raw_text = message.get("text") or ""

    normalized_text = raw_text.strip()

    if not message_id:
        return

    today = datetime.now(KST).date()

    started = time.time()

    ai_command = is_ai_command(
        normalized_text
    )

    normal_command = is_command(
        normalized_text
    )

    # -----------------------------------------------------
    # 일반 명령어 또는 AI 명령어일 때만
    # 동기 프로필 조회
    # -----------------------------------------------------

    should_force_profile = (
        ai_command
        or normal_command
    )

    fallback_name = (
        source.get("displayName")
        or f"사용자-{user_id[-6:]}"
    )

    display_name = get_display_name(
        group_id,
        user_id,
        fallback=fallback_name,
        force_fetch=should_force_profile,
    )

    # -----------------------------------------------------
    # 메시지 기록
    # -----------------------------------------------------

    character_count = len(
        re.sub(
            r"\s+",
            "",
            raw_text,
        )
    )

    try:
        record_message(
            event_id=message_id,
            group_id=group_id,
            user_id=user_id,
            display_name=display_name,
            message_date=today,
            character_count=character_count,
        )

    except Exception as e:
        logger.exception(
            "메시지 기록 실패: %s",
            e,
        )

    # -----------------------------------------------------
    # AI
    # -----------------------------------------------------

    if ai_command:

        question = normalized_text[1:].strip()

        if not question:
            reply_text(
                reply_token,
                "응? 🧸\n나한테 물어볼 말을 ! 뒤에 적어줘!",
            )
            return

        # reply token 저장
        CURRENT_REPLY_TOKEN.value = reply_token

        try:
            answer = ask_gemini(
                question
            )

            reply_text(
                reply_token,
                answer,
            )

        except Exception as e:
            logger.exception(
                "AI 처리 실패: %s",
                e,
            )

            reply_text(
                reply_token,
                "앗 장난감 가게 AI가 넘어졌어 🧸💦\n"
                "조금 있다가 다시 불러줘!",
            )

        finally:
            try:
                del CURRENT_REPLY_TOKEN.value
            except Exception:
                pass

        elapsed = time.time() - started

        if elapsed > 1.0:
            logger.warning(
                "AI 처리 지연: %.2f초, 질문='%s'",
                elapsed,
                normalized_text[:100],
            )

        return

    # -----------------------------------------------------
    # 일반 메시지
    # -----------------------------------------------------

    if not normal_command:

        # 일반 채팅은 DB 기록만 하고
        # 프로필은 백그라운드에서 갱신
        schedule_profile_lookup(
            group_id,
            user_id,
        )

        return

    # -----------------------------------------------------
    # 일반 명령어
    # -----------------------------------------------------

    CURRENT_REPLY_TOKEN.value = reply_token

    try:
        response = handle_command(
            normalized_text,
            group_id,
            user_id,
            display_name,
            today,
        )

        if response and response != "__MENTION_SENT__":
            reply_text(
                reply_token,
                response,
            )

    except Exception as e:
        logger.exception(
            "명령어 처리 실패: %s",
            e,
        )

        reply_text(
            reply_token,
            "앗 명령어 처리 중 문제가 생겼어 🧸💦",
        )

    finally:
        try:
            del CURRENT_REPLY_TOKEN.value
        except Exception:
            pass

    elapsed = time.time() - started

    if elapsed > 1.0:
        logger.warning(
            "명령어 처리 지연: %.2f초, 명령어='%s'",
            elapsed,
            normalized_text,
        )

    schedule_profile_lookup(
        group_id,
        user_id,
    )


# =========================================================
# Callback
# =========================================================

@app.post("/callback")
def callback():
    body