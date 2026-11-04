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

    if not group_id or not user_id or not message_id:
        logger.warning("필수 이벤트 정보가 없어 건너뜁니다.")
        return

    today = datetime.now(KST).date()
    # 프로필이 캐시에 없으면 임시 이름을 즉시 사용하고, 조회는 백그라운드로 진행합니다.
    display_name = get_display_name(source, group_id, user_id)
    character_count = len(re.sub(r"\s+", "", raw_text))

    # 마딧수는 일반 채팅도 기록해야 하므로 DB 기록은 유지합니다.
    # 연결 풀을 사용해 매번 DB 연결을 새로 여는 비용을 줄입니다.
    record_message(
        event_id=message_id,
        group_id=group_id,
        user_id=user_id,
        display_name=display_name,
        message_date=today,
        character_count=character_count,
    )
    # 핵심 최적화: 일반 채팅은 명령어 처리/정규식/통계 조회를 하지 않고 여기서 종료합니다.
    if not is_command(normalized_text):
        # 프로필 조회는 백그라운드로 예약하므로 일반 채팅 요청을 기다리게 하지 않습니다.
        schedule_profile_lookup(source, group_id, user_id)
        elapsed = time.monotonic() - started_at
        if elapsed >= 1.0:
            logger.warning("일반 메시지 기록 지연: %.2f초", elapsed)
        return

    reply = handle_command(
        normalized_text,
        group_id,
        user_id,
        today,
        display_name,
        event.get("replyToken"),
    )

    # 멘션 명령은 handle_command 내부에서 이미 답장했습니다.
    if reply and reply != "__MENTION_SENT__":
        reply_text(event.get("replyToken"), reply)

    # 명령어 처리(관리자/인증자 등록 포함)가 끝난 뒤 이름을 비동기로 보정합니다.
    schedule_profile_lookup(source, group_id, user_id)

    elapsed = time.monotonic() - started_at
    if elapsed >= 1.0:
        logger.warning("명령어 처리 지연: %.2f초, 명령어=%r", elapsed, normalized_text)


# ==================================================
# 직접 실행용
# Render 실행 명령: gunicorn app:app
# ==================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
