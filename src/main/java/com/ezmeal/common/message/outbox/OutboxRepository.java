package com.ezmeal.common.message.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import java.time.LocalDateTime;
import java.util.List;

/*
* [existsByAggregateIdAndStatusAndIdLessThan]
* -> 동일한 aggregateId에 대해서 순서를 보장하기 위해서
* -> 동일한 도메인 식별자(aggregateId)를 가진 메시지 중에서,
* -> 현재 처리하려는 메시지보다 먼저 생성 되었으나 (id < 현재 id)
* -> 아직 발송되지 않은 (INIT 상태인) 선행 메시지가 존재하는지 확인
*
* [findByStatusAndCreatedAtBeforeOrderByIdAsc]
* -> OutboxRelay(카프카 전송에 실패한 메시지들을 재전송하는 기능)에서 사용하기 위해
* -> Relay가 작동하는 시점(=threshold) 이전에 만들어졌으나,
* -> 아직 발송되지 못한 (INIT 상태인) 메시지를 조회
* -> 순서가 역전되는 것을 방지하기 위해 오래된 메시지부터 가져옴(=OrderByIdASC)
*
* */

public interface OutboxRepository extends JpaRepository<OutboxMessage, Long> {

    boolean existsByAggregateIdAndStatusAndIdLessThan(String aggregateId, OutboxStatus status, Long id);

    List<OutboxMessage> findByStatusAndCreatedAtBeforeOrderByIdAsc(OutboxStatus status, LocalDateTime threshold);
}
