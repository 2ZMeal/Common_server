package com.ezmeal.common.message.inbox;

import org.springframework.data.jpa.repository.JpaRepository;

/*
* event id를 기준으로 중복 수신되는 메시지인지 파악 (aggregate id 아님에 주의)
* InboxMessage 엔티티에 인덱스를 추가해뒀기에 많은 메시지가 쌓여도 조회 속도가 느리지 않도록 함
* */

public interface InboxRepository extends JpaRepository<InboxMessage, Long> {

    boolean existsByEventId(String eventId);
}
