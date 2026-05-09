package com.ezmeal.common.message.outbox;

/*
* INIT - DB에 최초로 저장하였으나, 카프카 발송을 하지 않은 대기 상태
* PUBLISHED - 카프카 메시지가 발송이 완료된 상태
* FAILED - 카프카 발송이 완전 실패한 경우 (추후 DLT 적용을 하는 경우 사용)
* */
public enum OutboxStatus {
    INIT,
    PUBLISHED,
    FAILED
}
