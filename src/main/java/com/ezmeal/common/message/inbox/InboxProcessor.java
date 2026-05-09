package com.ezmeal.common.message.inbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/*
* 각 도메인의 서비스 레이어에 있는 transaction과 inbox 저장을 하나의 트랜잭션으로 묶어줌
*
* [필독]
* 단, 기존 각 하위 애플리케이션의 KafkaEventLisenterImpl을 하단의 예시와 같이 inboxProcessor를 사용해야
* 하나의 트랜잭션으로 묶는게 가능함
*
* [KafkaEventListener 예시코드]
* @Component
* @RequiredArgsConstructor
* public class PaymentEventListener {
*
*     private final InboxProcessor inboxProcessor;
*     private final OrderService orderService; // 각 하위 애플리케이션의 서비스
*
*     @KafkaListener(topics = "payment-events-topic")
*     public void handlePayment(EventEnvelope<PaymentCompletedEvent> event) {
*
*         // inboxProcessor에게 "내가 실행할 로직(= businessLogic)"을 람다식을 통해 통째로 넘김
*         inboxProcessor.processOnce(event.eventId(), () -> {
*             PaymentCompletedEvent payload = event.payload();
*             orderService.completePayment(payload.getOrderId());
*         });
*     }
* }
*
*
* */

@Slf4j
@Component
@RequiredArgsConstructor
public class InboxProcessor {

    private final InboxRepository inboxRepository;

    /**
     * [주의]
     * processOnce를 사용하기에 앞서 &상단의 주석&을 꼭 참고바랍니다
     *
     * eventId - 카프카로 수신한 EventEnvelope의 고유 ID
     * businessLogic - 실제 수행해야 할 하위 서비스의 비즈니스 로직 (람다식)
     */
    @Transactional
    public void processOnce(String eventId, Runnable businessLogic) {

        // 이미 Inbox 테이블에 이 eventId가 존재하는 경우 (중복 수신)
        if (inboxRepository.existsByEventId(eventId)) {
            log.info("이미 처리된 이벤트입니다. 건너뜁니다. (중복 방지) eventId: {}", eventId);
            // 따로 처리하지 않아 멱등성 보장
            return;
        }

        // 처음 보는 메시지인 경우, Inbox DB에 Insert 함,
        // 0.00001초 차이로 와도
        // eventId에 걸어둔 Unique 제약으로 DB에서 예외가 터져 중복을 막음
        inboxRepository.save(new InboxMessage(eventId));

        // 람다식으로 넘겨준 비즈니스 로직(각 도메인의 service에 있는 transaction)을 실행합니다.
        businessLogic.run();

        // 각 도메인의 비즈니스 로직(transaction)이 에러 없이 무사히 끝나면,
        // Inbox 데이터가 함께 DB에 Commit 됨
    }
}
