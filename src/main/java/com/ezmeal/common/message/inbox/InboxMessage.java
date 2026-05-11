package com.ezmeal.common.message.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

/*
* [private Long id;]
* id(PK)는 들어온 순서를 파악하기 위해 Long으로 사용함
* 대신 event 고유 id는 별도의 필드 (eventId)에 저장
*
* [@Index(name = "idx_inbox_event_id", columnList = "eventId")]
* 수신된 이벤트 ID가 중복인지 검사할 때 빠르게 수행하기 위해 인덱스 생성
*
* */

@Entity
@Table(
        name = "common_inbox_message",
        indexes = {
                @Index(name = "idx_inbox_event_id", columnList = "eventId")
        }
)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class InboxMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false, updatable = false)
    private String eventId;

    @Column(nullable = false, updatable = false)
    private LocalDateTime processedAt;

    // 원본 traceId
    @Column
    private String traceId;

    public InboxMessage(String eventId, String traceId) {
        this.eventId = eventId;
        this.traceId = traceId;
        this.processedAt = LocalDateTime.now();
    }
}
