package com.ezmeal.common.message.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;

/*
 *[@Transactional(propagation = Propagation.REQUIRES_NEW)]
 * => 카프카 발행 성공 시, 이벤트(OutboxMessage)의 상태를 PUBLISHED로 변경
 *
 * [개선 사항]
 * 기존에는 DB에 저장된 JSON(String)을 EventEnvelope 객체로 역직렬화한 뒤 전송했으나,
 * 타입 소실 문제(LinkedHashMap 변환) 및 불필요한 오버헤드를 방지하기 위해
 * DB에 저장된 완전한 JSON 문자열(message.getPayload())을 그대로 Kafka로 전송합니다.
 *
 * */

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxMessageProcessor {

    private final OutboxRepository outboxRepository;
    // 더 이상 ObjectMapper가 필요 없으므로 삭제
    // 타입을 <String, String>으로 변경
    private final KafkaTemplate<String, String> kafkaTemplate;

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
                log.warn("순서 보장을 위해 발송 보류 (aggregateId: {}). 앞선 메시지부터 스케줄러가 순차 처리합니다.", message.getAggregateId());
                return;
            }

            try {
                // 역직렬화(ObjectMapper) 과정 삭제
                // DB에 있는 payload(JSON String)를 그대로 Record의 Value로 담음
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(message.getTopic(), message.getAggregateId(), message.getPayload());

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
                if (message.getTraceId() != null) {
                    record.headers().add("X-Original-Trace-Id", message.getTraceId().getBytes(StandardCharsets.UTF_8));
                }

                // 카프카 실제 발송 (이미 JSON 형태이므로 String 그대로 날아감)
                kafkaTemplate.send(record);

                // 발송 성공 시 DB 상태 변경
                message.markAsPublished();
                log.debug("Outbox 메시지 카프카 발행 성공. ID: {}", messageId);

            } catch (Exception e) {
                log.error("Outbox 메시지 카프카 발행 실패. Message ID: {}", messageId, e);
            }
        });
    }
}
