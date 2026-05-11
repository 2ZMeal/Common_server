package com.ezmeal.common.config;

import com.ezmeal.common.message.DomainEvent;
import com.ezmeal.common.message.EventEnvelope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/*
 * [처리 흐름 및 역할]
 * 1. application.yaml에 있는 StringSerializer를 적용한 ProducerFactory를
 *    가지고 KafkaTemplate<String, String>를 생성
 *
 * 2. OutboxMessageProcessor에서 DB에 저장된 JSON String 형태의 페이로드를
 *    객체 역직렬화 없이 그대로 Kafka로 전송하도록 String 타입으로 지정
 *
 * 3. setObservationEnabled를 통해 zipkin이 추적할 수 있도록 이벤트 옵션 활성화
 * */

@Configuration
public class KafkaProducerConfig {

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(
            ProducerFactory<String, String> producerFactory) {

        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);


        // zipkin을 위해 옵션 활성화
        kafkaTemplate.setObservationEnabled(true);

        return kafkaTemplate;
    }
}
