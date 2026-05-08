package com.ezmeal.common.message;

import java.time.LocalDateTime;
import java.util.UUID;

/*
* DomainEvent를 implements한 이벤트들(UserUpdatedEvent등)의 내용을 T에 담고,
* EventEnvelope 타입으로 최종 반환함, 자동으로 생성시점을 occuredAt에 기입.
*
* 실제 발행은 CommonKafkaEventPublisher를 통해 최종 수행
* */

public record EventEnvelope<T extends DomainEvent>(
        String eventId,
        String eventType,
        String aggregateId,
        LocalDateTime occurredAt,
        T payload
) {
    public static <T extends DomainEvent> EventEnvelope<T> of(
            String eventType,
            String aggregateId,
            T payload
    ) {
        return new EventEnvelope<>(
                UUID.randomUUID().toString(),
                eventType,
                aggregateId,
                LocalDateTime.now(),
                payload
        );
    }
}
