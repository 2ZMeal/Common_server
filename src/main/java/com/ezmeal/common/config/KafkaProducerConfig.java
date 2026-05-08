package com.ezmeal.common.config;

import com.ezmeal.common.message.DomainEvent;
import com.ezmeal.common.message.EventEnvelope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/*
 * [처리 흐름 및 역할]
 * 1. KafkaTemplate<String, EventEnvelope<? extends DomainEvent>> kafkaTemplate에서
 *     application.yaml에 있는 StringSerializer와 JsonSerializer를 적용한 ProducerFactory를
 *     가지고 KafkaTemplate를 생성
 *
 * 2. KafkaTemplate에 타입을 <String, EventEnvelope<? extends DomainEvent>>로 지정하여
 *     EventEnvelope로만 감싸졌으며, 페이로드가 DomainEvent를 구현(implements)한
 *     이벤트 클래스 객체만 발행할 수 있도록 제한
 *
 * 3. setObservationEnabled를 통해 zipkin이 추적할 수 있도록 이벤트 옵션 활성화
 *
 * 4. 타입 제약과 setObservationEnabled가 적용한 KafkaTemplate를 message 패키지에 있는
 *     CommonKafkaEventPublisher로 전달
 * */

@Configuration
public class KafkaProducerConfig {

    @Bean
    public KafkaTemplate<String, EventEnvelope<? extends DomainEvent>> kafkaTemplate(
            ProducerFactory<String, Object> producerFactory) {

        KafkaTemplate kafkaTemplate = new KafkaTemplate<>(producerFactory);

        // zipkin을 위해 옵션 활성화
        kafkaTemplate.setObservationEnabled(true);

        return kafkaTemplate;
    }
}
