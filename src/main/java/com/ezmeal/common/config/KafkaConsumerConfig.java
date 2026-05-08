package com.ezmeal.common.config;

import com.ezmeal.common.security.interceptor.KafkaSecurityInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

/*
 * [처리 흐름]
 * 1. Infra 레포지터리의 application.yaml에서 StringDeserializer를 통해 byte 데이터를
 *   JSON String으로 읽어옴
 *
 * 2. KafkaSecurityInterceptor가 카프카 헤더의 X-User-Id 등을 추출하여
 *   SecurityContextHolder에 추가함
 *
 * 3. setObservationEnabled를 통해 Zipkin이 추적할 수 있게 설정
 *
 * 4. StringJsonMessageConverter를 통해 JSON String을 가져오고,
 *   @KafkaListener 파라미터에 선언된 타입을 확인하여 역직렬화를 수행
 *   (JSON -> 각 객체로 변환)
 *
 *   [사용 예시 코드]
 *    public class UserEventListener {
 *
 *           @KafkaListener(topics = "user-events-topic", groupId = "notification-group")
 *           public void handleUserCreated(EventEnvelope<UserCreatedEvent> event) {
 *
 *               UserCreatedEvent payload = event.payload();
 *               sendWelcomeEmail(payload.email(), payload.name());
 *           }
 *    }
 *
 *   -> StringJsonMessageConverter가 EventEnvelope에 있는 UserCreatedEvent로 역직렬화를 수행
 *
 * */

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // Kafka 헤더의 정보를 Security Context Holder에 추가
        factory.setRecordInterceptor(new KafkaSecurityInterceptor<>());

        // Zipkin 분산 추적
        factory.getContainerProperties().setObservationEnabled(true);

        // String(JSON) 데이터를 @KafkaListener에 선언된 객체로 자동 변환해주는 컨버터
        factory.setRecordMessageConverter(new StringJsonMessageConverter());

        return factory;
    }
}
