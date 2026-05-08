package com.ezmeal.common.message;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/*
* [사용 예시 코드]
* public class UserService {

    // 공통 모듈에서 빈으로 등록한 CommonKafkaEventPublisher를 주입받음
    private final CommonKafkaEventPublisher eventPublisher;

    @Transactional
    public void createUser(String email, String name) {

        String newUserId = "fedsd-12df-ssdf-1egsd";

        // UserCreatedEvent는 DomainEvent 인터페이스를 implements 해야 함
        UserCreatedEvent payload = new UserCreatedEvent(newUserId, email, name);

        // 퍼블리셔를 통해 카프카 이벤트 발행
        // topic, key(aggregateId), eventType, payload
        eventPublisher.publish(
                "user-events-topic",    // topic 이름
                newUserId,              // 도메인 관련 uuid
                "USER_CREATED",         // eventType (어떤 사건인지 명시)
                payload                 // 실제 이벤트 데이터, 여기서는 UserCreatedEvent
        );
    }
}
* */

@Component
@RequiredArgsConstructor
public class CommonKafkaEventPublisher {

    // config에 있는 KafkaConsumerConfig(Zipkin을 적용한)를 가져와서 주입
    private final KafkaTemplate<String, EventEnvelope<? extends DomainEvent>> kafkaTemplate;

    public <T extends DomainEvent> void publish(
            String topic,
            /*
            * key에는 각 도메인에 연관된 id를 기록합니다.
            *
            * (예시)
            * userId, productId, orderId, paymentId 등
            *
            * */
            String key,
            /*
             * eventType에는 어떤 이벤트인지 설명을 기록합니다.
             *
             * (예시)
             * "USER_CREATED", "PAYMENT_CANCELLED" ...
             *
             * */
            String eventType,
            T payload
    ) {
        // EventEnvelop의 of를 통해 각 payload(T)에 맞게 EventEnvelop 객체를 생성
        EventEnvelope<T> envelope = EventEnvelope.of(eventType, key, payload);

        // envelop로 포장되어 최종 전달
        kafkaTemplate.send(topic, key, envelope);
    }
}
