package com.ezmeal.common.message.outbox;

import com.ezmeal.common.message.DomainEvent;
import com.ezmeal.common.message.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;

/*
 * [@Transactional(propagation = Propagation.REQUIRES_NEW)]
 * => 카프카 발행 성공 시, 이벤트(OutboxMessage)의 상태를 PUBLISHED로 변경
 *
 * (주의)
 * -> 기존 각 서비스 처리 내의 transaction(객체 생성, 수정, 삭제)과 별도의
 * -> 새로운 트랜잭션을 생성함, 해당 트랜잭션은 기존 각 서비스의 개별 객체에 대한 트랜잭션이 아닌
 * -> 발행해야 할 이벤트(OutBoxMessage)에 대한 transaction임을 혼동하지 말 것
 *
 * [private final ObjectMapper objectMapper]
 * -> OutboxMessage 엔티티의 payload는 모든 각 도메인의 Event dto에 맞게 저장할 수가 없음
 * -> 만약 모든 도메인의 Event dto를 저장하면 공통 모듈의 책임이 너무 커짐
 * -> 이 상황에서 OutboxMessage 엔티티는 payload를 String 형태로 저장해야 하는 상황에 처함
 *
 * -> 하지만 KafkaTemplate에는 Evelope<? extends DomainEvent>로 타입 검증이 작동하는 상태임으로
 * -> 컴파일러 타입 제약 조건을 통과하기 위해 objectMapper를 사용하여 EventEnvelope 객체 형태로 역직렬화함
 * -> 명시적 캐스팅을 수행하고, 컴파일러 에러를 무시하기 위해 "@SuppressWarnings("unchecked")"를 사용
 *
 * -> 카프카에서 보낼 때 타입체킹을 수행하지는 않지만,
 * -> 각 하위 서비스에서 CommonKafkaEventPublisher의
 * -> publish(topic, key, eventType, payload)를 호출할 때
 * -> 타입 체킹이 수행됨
 *
 * */


@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxMessageProcessor {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, EventEnvelope<? extends DomainEvent>> kafkaTemplate;
    // **objectMapper 쓰는 이유 상단 주석 참고**
    private final ObjectMapper objectMapper;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void process(Long messageId) {

        outboxRepository.findById(messageId).ifPresent(message -> {

            // 이미 처리해서 PUBLISHED 상태라면 무시 (중복 방지)
            if (message.getStatus() == OutboxStatus.PUBLISHED) {
                return;
            }

            // 먼저 생성된(id가 더 작은) 동일 도메인(aggregateId)의 INIT(발행 못한) 메시지가 있는지 확인
            boolean hasOlderPending = outboxRepository.existsByAggregateIdAndStatusAndIdLessThan(
                    message.getAggregateId(), OutboxStatus.INIT, messageId
            );

            if (hasOlderPending) {
                // 앞선 이벤트가 발송 실패해서 대기 중이므로, 지금 이 이벤트를 쏘면 수신자 측에서 순서가 역전되게 됨
                log.warn("순서 보장을 위해 발송 보류 (aggregateId: {}). 앞선 메시지부터 스케줄러가 순차 처리합니다.", message.getAggregateId());
                /*
                * 발송을 취소하고(계속 INIT 상태로 남김) 앞의 메시지가 처리된 이후에 발행됨
                *
                * 앞의 메시지가 처리되면, 다음번 relay(스케줄러)가 작동할 때 다시 발행해줌
                *
                * 스케줄러는 특정 주기마다 반복해서 작동함, 동일한 aggregateId에 대해 다소 지연이 생기더라도
                * 동일한 aggregateId에 대해서는 이벤트 발행 순서를 보장해줘야 함
                * -> 만약 발행 쪽에서 처리 순서를 보장해주지 않으면,
                * -> 각 도메인에서 처리 순서가 중요한 엣지 케이스에 대한 handling을 해줘야 함
                *
                * (엣지 케이스 예시)
                * 객체 생성되어 생성 이벤트 발행 이후 삭제해서 삭제 이벤트를 발행했는데,
                * 처음 생성 이벤트 전송이 실패하여 삭제 이벤트를 먼저 발행하고 생성 이벤트를 발행하여
                * 최종 결과는 해당 객체가 없어야 하는데 객체가 생성되는 문제
                *
                * */
                return;
            }

            try {
                // 최상단 "[private final ObjectMapper objectMapper]" 주석 설명 참고
                @SuppressWarnings("unchecked")
                EventEnvelope<? extends DomainEvent> envelope =
                        (EventEnvelope<? extends DomainEvent>) objectMapper.readValue(message.getPayload(), EventEnvelope.class);

                // 템플릿의 제약 조건을 만족하며 카프카 전송용 Record를 조립
                ProducerRecord<String, EventEnvelope<? extends DomainEvent>> record =
                        new ProducerRecord<>(message.getTopic(), message.getAggregateId(), envelope);

                // 보안 헤더 추가
                if (message.getUserId() != null) {
                    record.headers().add("X-User-Id", message.getUserId().getBytes(StandardCharsets.UTF_8));
                }
                if (message.getUserRole() != null) {
                    record.headers().add("X-User-Roles", message.getUserRole().getBytes(StandardCharsets.UTF_8));
                }
                if (message.getUserEmail() != null) {
                    record.headers().add("X-User-Email", message.getUserEmail().getBytes(StandardCharsets.UTF_8));
                }

                // 카프카 실제 발송
                // 인프라의 application.yaml에 적어둔 JsonSerializer가 동작하여 직렬화
                kafkaTemplate.send(record);

                // 발송 성공 시 DB 상태 변경 (메서드 종료 시 더티체킹으로 자동 UPDATE 쿼리 발생)
                message.markAsPublished();
                log.debug("Outbox 메시지 카프카 발행 성공. ID: {}", messageId);

            } catch (Exception e) {
                // 에러를 밖으로 던지지 않아야 트랜잭션 롤백이 되지 않고 INIT 상태가 유지되고,
                // 스케줄러가 다음번에 작동할 때 재시도가 가능함
                log.error("Outbox 메시지 카프카 발행 실패. Message ID: {}", messageId, e);
            }
        });
    }
}
