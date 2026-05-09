package com.ezmeal.common.message.outbox;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Outbox 트랜잭션 커밋이 완료된 시점에 최종적으로 카프카 메시지를 발행하라는
 * 스프링 내부 이벤트를 발행할 때 사용하는 스프링 내부 이벤트용 DTO
 *
 * outboxMessageId - 커밋이 완료된 OutBoxMessage 엔티티의 id (순서 id)
 */
@Getter
@AllArgsConstructor
public class OutboxInternalEvent {

    private final Long outboxMessageId;

}
