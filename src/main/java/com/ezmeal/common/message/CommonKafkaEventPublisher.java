package com.ezmeal.common.message;

import com.ezmeal.common.message.outbox.OutboxInternalEvent;
import com.ezmeal.common.message.outbox.OutboxMessage;
import com.ezmeal.common.message.outbox.OutboxRepository;
import com.ezmeal.common.security.principal.CustomUserPrincipal;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Tracer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/*
 *[Outbox 패턴이 적용된 이벤트 발행 처리 흐름]
 * 1. 하위 서비스의 @Transactional 메서드 내에서 publish()가 호출됨.
 * 2. 카프카로 즉시 발송하지 않고, OutboxMessage 엔티티로 변환하여 DB에 저장 (상태: INIT).
 *    (이때 하위 서비스의 비즈니스 데이터와 이벤트 데이터가 하나의 DB 트랜잭션으로 완벽히 묶임)
 * 3. 스프링 내부 이벤트(ApplicationEvent)를 발행하여 발송 대기 상태로 만듦.
 * 4. 하위 서비스의 비즈니스 트랜잭션이 성공적으로 종료(Commit)됨.
 * 5. Commit 직후, OutboxCommitListener가 비동기로 카프카에 전송하고 상태를 PUBLISHED로 변경.
 *    (만약 실패하거나 앞선 메시지가 있다면, 1분마다 도는 스케줄러가 순서에 맞게 재시도함)
 *
 *[사용 예시 코드 - 하위 애플리케이션(UserService.java)]
 *
 * @Service
 * @RequiredArgsConstructor
 * public class UserService {
 *
 *     private final CommonKafkaEventPublisher eventPublisher;
 *
 *     // 주의) 반드시 @Transactional이 붙어있어야 함
 *     @Transactional
 *     public void createUser(String email, String name) {
 *
 *         // 실제 비즈니스 로직 처리 및 DB 저장
 *         User newUser = new User(email, name);
 *         userRepository.save(newUser);
 *
 *         // 카프카로 보낼 이벤트 객체 생성 (DomainEvent 인터페이스 구현 필수)
 *         UserCreatedEvent payload = new UserCreatedEvent(newUser.getId(), email, name);
 *
 *         // 퍼블리셔를 통해 이벤트 발행 (내부적으로 Outbox DB에 Insert 됨)
 *         eventPublisher.publish(
 *                 "user-events-topic",    // topic 이름
 *                 newUser.getId(),        // aggregateId (순서 보장용 기준 키)
 *                 "USER_CREATED",         // eventType (어떤 사건인지 명시)
 *                 payload                 // 실제 이벤트 데이터
 *         );
 *     }
 * }
 *
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class CommonKafkaEventPublisher {

    private final OutboxRepository outboxRepository;
    // ObjectMapper를 사용하는 이유는 OutboxMessageProcessor의 최상단 주석 참고
    private final ObjectMapper objectMapper;

    // 스프링 내부 이벤트를 던지기 위한 퍼블리셔 (스프링 기본 제공 Bean)
    private final ApplicationEventPublisher applicationEventPublisher;

    // Tracer 추가 (현재 스레드의 Trace 정보 추출용)
    private final Tracer tracer;

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
        // EventEnvelope 객체 생성 (고유 eventId, 생성 시간 등이 자동 세팅됨)
        // EventEnvelop의 of를 통해 각 payload(T)에 맞게 EventEnvelop 객체를 생성
        EventEnvelope<T> envelope = EventEnvelope.of(eventType, key, payload);

        // SecurityContext에서 유저 정보 추출
        // *비동기 스케줄러가 카프카 전송 시 헤더에 복구하기 위해 DB에 함께 저장해야 함
        String userId = null;
        String role = null;
        String email = null;

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.isAuthenticated() && auth.getPrincipal() instanceof CustomUserPrincipal principal) {
            userId = principal.getUserId();
            role = principal.getRole().name();
            email = principal.getEmail();
        }

        // 현재 스레드의 Zipkin TraceId 추출
        String traceId = null;
        if (tracer != null && tracer.currentSpan() != null) {
            traceId = tracer.currentSpan().context().traceId();
        }

        try {
            // 객체를 JSON 문자열로 직렬화
            String jsonPayload = objectMapper.writeValueAsString(envelope);

            // 카프카로 바로 발행하지 않고 DB(OutboxMessage 테이블)에 엔티티로 저장 (상태: INIT)
            OutboxMessage outboxMessage = OutboxMessage.builder()
                    .topic(topic)
                    .aggregateId(key)
                    .eventType(eventType)
                    .eventId(envelope.eventId()) // 수신자 측에서 Inbox로 저장할 때 중복 체크할 키
                    .payload(jsonPayload)
                    .userId(userId)
                    .userRole(role)
                    .userEmail(email)
                    .traceId(traceId) // 추출한 traceId DB에 저장
                    .build();

            // 트랜잭션 안에서 DB Insert 발생
            outboxMessage = outboxRepository.save(outboxMessage);

            // 스프링 내부 이벤트 발행
            // (이 시점엔 카프카로 안 보내지고. 호출한 쪽의 DB 트랜잭션이 완벽히 커밋될 때까지 대기)
            applicationEventPublisher.publishEvent(new OutboxInternalEvent(outboxMessage.getId()));

        } catch (JsonProcessingException e) {
            log.error("이벤트 직렬화 실패 - topic: {}, key: {}", topic, key, e);
            throw new RuntimeException("이벤트 직렬화 실패", e);
        }
    }
}
