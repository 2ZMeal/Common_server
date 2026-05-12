package com.ezmeal.common.config;

import com.ezmeal.common.security.interceptor.KafkaSecurityInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

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
 * 5. DefaultErrorHandler를 통해 @KafkaListener 처리 중 발생한 예외를 공통으로 처리함
 *   - FixedBackOff 설정에 따라 일정 간격으로 재시도 수행
 *   - 현재 설정은 1초 간격으로 3회 재시도
 *   - 재시도 후에도 계속 실패하면 DeadLetterPublishingRecoverer가 동작함
 *   - 실패한 원본 메시지는 기존 토픽명 뒤에 ".DLT"를 붙인 토픽으로 발행됨
 *     예) order.cancelled -> order.cancelled.DLT
 *   - DLT 발행에는 KafkaTemplate<String, String>을 사용함
 *
 *   [주의]
 *   @KafkaListener 내부에서 예외를 catch만 하고 다시 throw하지 않으면
 *   Listener Container가 실패를 감지하지 못하므로 DLT로 이동하지 않음
 *
 *   [잘못된 예시 - 예외를 삼켜 DLT가 동작하지 않음]
 *    @KafkaListener(topics = "order.cancelled", groupId = "product-group")
 *    public void handleOrderCancelled(EventEnvelope<OrderCancelledMessage> event) {
 *        try {
 *            OrderCancelledMessage payload = event.payload();
 *            productService.restoreReservedQuantity(payload.productId(), payload.orderId());
 *        } catch (Exception e) {
 *            log.error("재고 복구 실패", e);
 *        }
 *    }
 *
 *   [올바른 예시 - 예외를 다시 던져 DLT 정책이 동작함]
 *    @KafkaListener(topics = "order.cancelled", groupId = "product-group")
 *    public void handleOrderCancelled(EventEnvelope<OrderCancelledMessage> event) {
 *        try {
 *            OrderCancelledMessage payload = event.payload();
 *            productService.restoreReservedQuantity(payload.productId(), payload.orderId());
 *        } catch (Exception e) {
 *            log.error("재고 복구 실패", e);
 *            throw e;  <---------------------
 *        }
 *    }
 * */

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler
    ) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // Kafka 헤더의 정보를 Security Context Holder에 추가
        factory.setRecordInterceptor(new KafkaSecurityInterceptor<>());

        // Zipkin 분산 추적
        factory.getContainerProperties().setObservationEnabled(true);

        // String(JSON) 데이터를 @KafkaListener에 선언된 객체로 자동 변환해주는 컨버터
        factory.setRecordMessageConverter(new StringJsonMessageConverter());

        // @KafkaListener 처리 중 예외 발생 시 공통 재시도/DLT 정책 적용
        factory.setCommonErrorHandler(kafkaErrorHandler);

        return factory;
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        // 최종 실패한 메시지를 원본 토픽명 + ".DLT" 토픽으로 발행
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) ->
                                new TopicPartition(record.topic() + ".DLT", record.partition())
                );

        // 1초 간격으로 3회 재시도 후 recoverer 실행
        FixedBackOff backOff = new FixedBackOff(1000L, 3L);

        return new DefaultErrorHandler(recoverer, backOff);
    }

    /*
     * DLT 토픽을 소비하는 @KafkaListener 전용 factory.
     *
     * 일반 consumer용 kafkaListenerContainerFactory와 달리
     * DefaultErrorHandler를 등록하지 않는다.
     *
     * 이유:
     * - DLT 메시지 처리 중 다시 예외가 발생했을 때
     *   원본토픽.DLT.DLT 같은 중첩 DLT 토픽이 생기는 것을 방지하기 위함
     *
     * 사용 예시:
     * @KafkaListener(
     *     topics = "order.cancelled.DLT",
     *     groupId = "product-dlt-group",
     *     containerFactory = "kafkaDltListenerContainerFactory"
     * )
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaDltListenerContainerFactory(
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
