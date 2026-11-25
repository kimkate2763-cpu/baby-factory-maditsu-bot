import os
import hmac
import hashlib
import base64
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

logger = logging.getLogger(__name__)


# =========================================================
# 환경변수
# =========================================================

LINE_CHANNEL_SECRET = os.getenv(
    "LINE_CHANNEL_SECRET", ""
).strip()

LINE_CHANNEL_ACCESS_TOKEN = os.getenv(
    "LINE_CHANNEL_ACCESS_TOKEN", ""
).strip()

DATABASE_URL = os.getenv(
    "DATABASE_URL", ""
).strip()

GEMINI_API_KEY = os.getenv(
    "GEMINI_API_KEY", ""
).strip()


KST = ZoneInfo("Asia/Seoul")


# =========================================================
# Gemini 설정
# =========================================================

# 가장 먼저 시도할 모델
# 성공한 모델은 아래 순서보다 우선 사용됨.
GEMINI_MODELS = [
    "gemini-3.8-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash-lite",
]

# 연결 자체가 오래 걸리지 않도록 제한
GEMINI_CONNECT_TIMEOUT = 1.0

# 한 모델이 너무 오래 붙잡고 있지 않도록 제한
GEMINI_READ_TIMEOUT = 2.8

# 실패한 모델은 잠시 건너뜀
GEMINI_FAILURE_COOLDOWN = 60

# 3.8을 다시 확인하는 최소 간격
GEMINI_PREFERRED_RETRY_COOLDOWN = 180


_gemini_lock = threading.RLock()

_gemini_preferred_model = None

_gemini_failed_until = {}

_gemini_last_success = {}

_gemini_last_preferred_probe = 0.0


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
# DB 연결
# =========================================================

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
        and now - _db_connection_created_at
        > DB_RECONNECT_AFTER
    ):
        logger.info("DB 연결 재생성")
        _close_db_connection_locked()

    if _db_connection is None:

        if not DATABASE_URL:
            raise RuntimeError(
                "DATABASE_URL 환경변수가 없습니다."
            )

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
                    f"SET statement_timeout = "
                    f"{DB_STATEMENT_TIMEOUT}"
                )

            _db_connection.commit()

        except Exception:

            try:
                _db_connection.rollback()
            except Exception:
                pass

    return _db_connection


# =========================================================
# DB 테이블 초기화
# =========================================================

def _create_database_tables():
    global _db_initialized

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

    with _db_lock:

        conn = _get_connection_locked()

        try:

            with conn.cursor() as cur:
                cur.execute(sql)

            conn.commit()

            _db_initialized = True

            logger.info("DB 테이블 초기화 완료")

        except Exception:

            try:
                conn.rollback()
            except Exception:
                pass

            raise


def init_database():
    global _db_initialized

    if _db_initialized:
        return

    with _db_init_lock:

        if _db_initialized:
            return

        _create_database_tables()

        # 저장된 Gemini 우선 모델 복구
        try:

            row = db_fetchone(
                """
                SELECT state_value
                FROM bot_runtime_state
                WHERE state_key =
                    'gemini_preferred_model'
                """
            )

            if row:

                model = row.get(
                    "state_value"
                )

                if model in GEMINI_MODELS:

                    global _gemini_preferred_model

                    with _gemini_lock:
                        _gemini_preferred_model = model

                    logger.info(
                        "저장된 Gemini 우선 모델 복구: %s",
                        model,
                    )

        except Exception as e:

            logger.warning(
                "Gemini 우선 모델 복구 실패: %s",
                e,
            )


# =========================================================
# DB 실행
# =========================================================

def db_execute(
    sql,
    params=None,
    fetch=False,
):
    global _db_initialized

    init_database()

    for attempt in range(2):

        try:

            with _db_lock:

                conn = _get_connection_locked()

                with conn.cursor() as cur:

                    cur.execute(
                        sql,
                        params,
                    )

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
                "DB 연결 오류 "
                "(%s/2): %s",
                attempt + 1,
                e,
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


def db_fetchone(
    sql,
    params=None,
):
    rows = db_execute(
        sql,
        params,
        fetch=True,
    )

    if not rows:
        return None

    return rows[0]


def db_fetchall(
    sql,
    params=None,
):
    return (
        db_execute(
            sql,
            params,
            fetch=True,
        )
        or []
    )


# =========================================================
# Gemini 상태 저장
# =========================================================

def save_preferred_gemini_model(
    model,
):
    global _gemini_preferred_model

    if model not in GEMINI_MODELS:
        return

    with _gemini_lock:
        _gemini_preferred_model = model

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
                state_value =
                    EXCLUDED.state_value,
                updated_at = NOW()
            """,
            (model,),
        )

    except Exception as e:

        logger.warning(
            "Gemini 우선 모델 저장 실패: %s",
            e,
        )


def mark_gemini_failure(
    model,
):
    with _gemini_lock:

        _gemini_failed_until[model] = (
            time.time()
            + GEMINI_FAILURE_COOLDOWN
        )


def mark_gemini_success(
    model,
):
    global _gemini_last_preferred_probe

    with _gemini_lock:

        _gemini_failed_until.pop(
            model,
            None,
        )

        _gemini_last_success[model] = (
            time.time()
        )

        _gemini_last_preferred_probe = (
            time.time()
        )

    save_preferred_gemini_model(
        model
    )


def get_gemini_model_order():

    now = time.time()

    with _gemini_lock:

        preferred = (
            _gemini_preferred_model
        )

        candidates = []

        if preferred in GEMINI_MODELS:
            candidates.append(
                preferred
            )

        for model in GEMINI_MODELS:

            if model not in candidates:
                candidates.append(
                    model
                )

        available = []

        for model in candidates:

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

        # 모두 cooldown이면
        # 가장 빨리 다시 시도 가능한 모델 사용
        if not available:

            available = sorted(
                candidates,
                key=lambda model:
                    _gemini_failed_until.get(
                        model,
                        0,
                    ),
            )

        return available


# =========================================================
# Gemini Prompt
# =========================================================

GEMINI_SYSTEM_PROMPT = """
너는 'Baby Factory' LINE 단체방의
귀엽고 어설픈 장난감 가게 AI 집사야.

분위기:
- 장난감 가게에서 튀어나온 듯한 귀엽고 엉뚱한 느낌
- 조금 어설프지만 친절하고 센스 있음
- 너무 유치하지 않음
- 자연스러운 한국어
- 20대 느낌의 편한 말투
- 과한 존댓말 금지
- 필요하면 ㅋㅋ, ㅎㅎ, 이모지를 적당히 사용
- 최대 10줄
- 불필요한 장황한 설명 금지
- 질문에 필요한 핵심은 빠뜨리지 않기

중요:
- 모르는 내용을 사실처럼 만들어내지 마.
- 최신 정보가 필요한 질문은 검색 결과를 우선해.
- 날씨, 현재 가격, 영업시간, 최신 뉴스,
  맛집, 예약, 주식, 환율 등은 최신 정보가 필요할 수 있어.
- 검색 결과가 없으면 현재 정보인 것처럼 단정하지 마.
- 단순한 대화에는 자연스럽게 대답해.
- 의학/법률/금전 문제는 확정적으로 단정하지 마.
- 단체방 규칙을 물어봤는데 확실한 정보가 없다면
  임의로 규칙을 만들지 말고 관리자에게 확인하라고 해.
- 개인정보를 추측하거나 만들어내지 마.

점메추:
- 메뉴를 몇 개 바로 추천해.
- 너무 길게 설명하지 마.

밸런스게임:
- 재미있는 선택지를 만들어.
"""


# =========================================================
# Gemini 검색 필요 여부
# =========================================================

def _gemini_needs_search(
    text,
):
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
        "이번 주",
        "이번달",
        "이번 달",
        "내일",
        "모레",
    ]

    return any(
        keyword in lower
        for keyword in keywords
    )


# =========================================================
# Gemini 응답 텍스트 추출
# =========================================================

def _extract_gemini_text(
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
# Gemini 줄 수 제한
# =========================================================

def _limit_ai_lines(
    text,
    max_lines=10,
):
    if not text:
        return ""

    text = (
        text
        .replace("\r\n", "\n")
        .replace("\r", "\n")
    )

    lines = [
        line.rstrip()
        for line in text.split("\n")
        if line.strip()
    ]

    if len(lines) <= max_lines:
        return "\n".join(
            lines
        ).strip()

    return "\n".join(
        lines[:max_lines]
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
        GEMINI_SYSTEM_PROMPT
        + "\n\n사용자 질문:\n"
        + question
        + "\n\n답변 규칙:"
        + "\n- 최대 10줄"
        + "\n- 질문에 바로 답하기"
        + "\n- 불필요한 서론 금지"
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

    elapsed = (
        time.time()
        - started
    )

    if response.status_code != 200:

        body = response.text[:1200]

        raise RuntimeError(
            f"HTTP {response.status_code}: "
            f"{body}"
        )

    try:

        data = response.json()

    except Exception:

        raise RuntimeError(
            "Gemini 응답 JSON 파싱 실패"
        )

    text = _extract_gemini_text(
        data
    )

    if not text:

        raise RuntimeError(
            "Gemini 응답에 텍스트가 없습니다."
        )

    text = _limit_ai_lines(
        text,
        10,
    )

    logger.info(
        "Gemini 성공: "
        "model=%s, %.2fs, search=%s",
        model,
        elapsed,
        use_search,
    )

    return text


# =========================================================
# Gemini 질문
# =========================================================

def ask_gemini(
    question,
):
    """
    Gemini fallback.

    기본:
        3.8 → 3.6 → 3.5 Lite

    성공 모델:
        다음 요청에서 최우선

    실패 모델:
        60초 cooldown

    3.8이 다시 살아났을 때:
        일정 시간 후 다시 우선 사용
    """

    if not GEMINI_API_KEY:

        return (
            "앗 장난감 AI 키가 아직 안 꽂혔어 🧸\n"
            "관리자에게 GEMINI_API_KEY 설정을 "
            "확인해달라고 해줘!"
        )

    use_search = (
        _gemini_needs_search(
            question
        )
    )

    models = get_gemini_model_order()

    if not models:

        return (
            "앗 장난감 가게 AI들이 "
            "전부 잠깐 졸고 있어 🧸💤"
        )

    last_error = None

    # -----------------------------------------------------
    # 첫 번째 모델
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

    except Exception as e:

        last_error = e

        logger.warning(
            "Gemini 1차 실패: "
            "model=%s error=%s",
            first_model,
            e,
        )

        mark_gemini_failure(
            first_model
        )

    # -----------------------------------------------------
    # 나머지 모델
    #
    # 한 모델씩 기다리지 않고 동시에 시도해서
    # 먼저 성공한 모델을 사용한다.
    # -----------------------------------------------------

    remaining = [
        model
        for model in models[1:]
        if model != first_model
    ]

    if not remaining:

        return (
            "앗 장난감 가게 AI가 "
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

    future_map = {}

    try:

        for model in remaining:

            future = executor.submit(
                call_gemini_model,
                model,
                question,
                use_search,
            )

            future