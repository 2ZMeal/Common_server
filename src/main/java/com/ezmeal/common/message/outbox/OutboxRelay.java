package com.ezmeal.common.message.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/*
* 이벤트 발행에 실패한 이벤트들(= init 상태로 남은 이벤트들)을 다시
* messoageProcessor를 호출하여 재발송하는 스케줄러
*
* fixedDelay에 있는 시간 기간으로 작동, 초기값 = 10초
*
* 스케줄러가 작동되는 시점에서 기준으로 삼는 시각
* (초기값 10초임으로 스케줄러 작동 시각에서 10초 전)을 설정하고,
* 기준 시각 이전에 있던 메시지 중에서 init 상태는 메시지만 오랜된 순서로 가져오고
* 다시 메시지 전송을 시도함
*
* [특이사항]
* 추후 동일한 도메인 애플리케이션을 수평으로 확장할 경우,
* 각 애플리케이션 별로 스케줄러 중복 실행을 막기 위해 redis 기반 분산락 적용 등을 고려해야 함
*
* */

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxRepository outboxRepository;
    private final OutboxMessageProcessor messageProcessor;

    @Scheduled(fixedDelay = 10000)
    public void publishPendingMessages() {

        // 10초(기준시간) 이내의 메시지들은 방금 막 트랜잭션(=OutboxMessage)이 끝났거나,
        // 리스너(= OutboxCommitListener)가 처리 중인 정상 메시지이므로 처리하지 않음
        LocalDateTime tenSecondsAgo = LocalDateTime.now().minusSeconds(10);

        // aggregateId가 같은 경우는 순서를 보장하기 위해 반드시 오래된 메시지부터(OrderByIdAsc) 가져옴
        List<OutboxMessage> pendingMessages = outboxRepository
                .findByStatusAndCreatedAtBeforeOrderByIdAsc(OutboxStatus.INIT, tenSecondsAgo);

        if (!pendingMessages.isEmpty()) {
            log.warn("[Outbox] 미발송 메시지 {} 건 재시도 시작", pendingMessages.size());

            for (OutboxMessage message : pendingMessages) {
                // List에 가장 오래된 순서부터 담았기에 같은 aggregateId에 대해 순서가 보장되어 수행됨
                messageProcessor.process(message.getId());
            }
        }
    }
}
