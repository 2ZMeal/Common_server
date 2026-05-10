package com.ezmeal.common.message;

/*
    도메인 이벤트라는 것을 표시하기 위한 용도,
    실제 양식은 EventEnvelop와 CommonKafkaEventPublisher를 통함
*/

import com.fasterxml.jackson.annotation.JsonTypeInfo;

// 인터페이스를 구현한 객체가 직렬화될 때 구현한 객체의 클래스(implements한 클래스의 정보를) 알 수 있도록 어노테이션 추가
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public interface DomainEvent {
}
