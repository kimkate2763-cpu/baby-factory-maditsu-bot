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
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
KST = ZoneInfo("Asia/Seoul")

http = requests.Session()
_db_init_lock = threading.Lock()
_db_initialized = False

# DB 연결을 매 메시지마다 새로 만들지 않도록 연결 풀을 사용합니다.
_db_pool = None
if DATABASE_URL:
    _db_pool = ConnectionPool(
        conninfo=DATABASE_URL,
        min_size=1,
        max_size=8,
        timeout=5,
        kwargs={"row_factory": dict_row, "connect_timeout": 5},
        open=True,
    )

# 프로필 조회는 별도 작업 스레드에서 처리해 LINE API 지연이 웹훅을 막지 않게 합니다.
_PROFILE_CACHE = {}
_PROFILE_CACHE_LOCK = threading.Lock()
_PROFILE_CACHE_TTL = 600  # 10분
_PROFILE_LOOKUPS_IN_FLIGHT = set()
_PROFILE_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="line-profile")


# ==================================================
# 데이터베이스 연결 및 초기화
# ==================================================

def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL 환경변수가 설정되지 않았습니다.")
    if _db_pool is None:
        raise RuntimeError("데이터베이스 연결 풀이 초기화되지 않았습니다.")
    return _db_pool.connection(timeout=5)


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
