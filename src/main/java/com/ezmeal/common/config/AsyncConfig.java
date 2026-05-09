package com.ezmeal.common.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

/*
* message/outbox/OutboxCommitListener에서
* 메인 서비스 처리 흐름과 카프카 발행 처리 흐름을 비동기로 분리하기 위해
*
* @Async를 사용하는데,
* 하위 애플리케이션에서도 별도로 설정하지 않고 자동으로 @Async를 사용할 수
* 있도록 AsyncConfig(지금 이 파일)을 작성
* */

@EnableAsync
@Configuration
public class AsyncConfig {
}
