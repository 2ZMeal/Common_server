package com.ezmeal.common.message.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/*
* [@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)]
* => 카프카 메시지를 보내라는 내부 이벤트가 발행되면, 해당 이벤트를 캐치하는 Listener
*
* -> 최종 Outbox 엔티티가 생성되고, 스프링 내부 이벤트로 AFTER_COMMIT을 발행하면,
* -> 카프카 메시지를 보내주는 OutboxMessageProcessor의 process를 호출하여
* -> 실제 카프카 메시지를 보내도록 함
*
* [@Async]
* => 실제 서비스 처리 흐름에서 카프카 통신으로 서비스 처리가 지연되지 않도록 하기 위해,
* => 서비스 처리 흐름과 이벤트 발행 흐름을 비동기로 분리함
*
* -> 최종 이벤트 발행까지 끝나야 서비스 처리 흐름이 끝나도록 하지 않고,
* -> 서비스 처리 흐름(=메인 스레드)은 우선 처리하고
* -> 별도로 이벤트 발행 흐름(=별도 스레드)을 분리함(=비동기)
*
* (cf) 각 하위 서비스들에서 @Async를 사용할 수 있도록 (활성화를 위해)
* config 팩토리에 AsyncConfig를 추가함
*
* */

@Component
@RequiredArgsConstructor
public class OutboxCommitListener {

    private final OutboxMessageProcessor messageProcessor;

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleAfterCommit(OutboxInternalEvent event) {

        // 카프카 메시지 발행 시작
        messageProcessor.process(event.getOutboxMessageId());
    }
}
