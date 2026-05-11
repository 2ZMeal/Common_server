package com.ezmeal.common.message.outbox;

import com.ezmeal.common.entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/*
* 이벤트 발행 순서를 파악하기 위해 id를 Long으로 지정함
* -> 모든 이벤트를 순서대로 보내는 것이 아님,
* -> 동일한 aggregateId에 대해서만 이벤트 순서를 보장할 수 있도록 하기 위함
*
* (예시)
* 상품 생성 이벤트 발생 이후 상품 삭제 이벤트 발생하였으나,
* 특정 상품(=aggreageId)에 대한 상품 생성 이벤트가 제대로 전달이 못되고
* 삭제가 먼저 전달되면 문제가 발생할 수 있음
*
* 다른 aggregateId에 대해서는 순서를 보장하지 않아서 같은 aggreageId가 아니면
* 병목이 생기지는 않음
*
* 이벤트 발행이 실패할 때 다시 보내는 스케줄러가 작동할 때,
* 스케줄러의 빠른 조회를 위해 사전에 복합 인덱스(aggregateId, status, id)를 설정
*
* */

@Entity
@Table(
        name = "common_outbox_message",
        indexes = {
                @Index(name = "idx_outbox_agg_status_id", columnList = "aggregateId, status, id")
        }
)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxMessage extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // 발행한 이벤트에 대한 정보들
    @Column(nullable = false)
    private String topic;

    @Column(nullable = false)
    private String aggregateId;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false, unique = true)
    private String eventId;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;

    // 현재 발행 상태
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    // 원본 traceId 기록 (비동기 스레드에서 수행되기 때문)
    @Column
    private String traceId;

    /*
    * 비동기 스케줄러(Relay)가 카프카 헤더에 X-User-Id 등을 복구하기 위한 필드
    * 시스템이 발행하는 이벤트일 수도 있기에 nullable 허용
    * */
    @Column
    private String userId;

    @Column
    private String userRole;

    @Column
    private String userEmail;

    /*
    * 최초 생성할 때는 무조건 초기 상태(status)를 INIT으로 하도록 설정
    * */
    @Builder
    public OutboxMessage(String topic, String aggregateId, String eventType, String eventId, String payload, String userId, String userRole, String userEmail, String traceId) {
        this.topic = topic;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.eventId = eventId;
        this.payload = payload;
        this.userId = userId;
        this.userRole = userRole;
        this.userEmail = userEmail;
        this.traceId = traceId;
        this.status = OutboxStatus.INIT;
    }

    /*
    * 카프카 발행을 최종 성공하면 상태(status)를 PUBLISHED로 변경
    * */
    public void markAsPublished() {
        this.status = OutboxStatus.PUBLISHED;
    }
}
