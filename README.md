<br>
<h1 align="center">🍱 EZMeal-Common-Module <span style="font-size:1.2em;">🍱</span></h1>

<p align="center">
  <a href="https://spring.io/projects/spring-boot">
  <img src="https://img.shields.io/static/v1?label=Spring%20Boot&message=3.5&color=green&logo=springboot" alt="Spring Boot">
  </a>
  <a href="https://www.java.com/">
  <img src="https://img.shields.io/static/v1?label=Java&message=21&color=orange&logo=openjdk" alt="Java">
  </a>
  <a href="https://www.mysql.com/">
  <img src="https://img.shields.io/static/v1?label=Postgres&message=16&color=blue&logo=mysql" alt="MySQL">
  </a>
  <a href="https://kafka.apache.org/">
  <img src="https://img.shields.io/static/v1?label=Apache%20Kafka&message=Messaging&color=black&logo=apachekafka" alt="Kafka">
  </a>
</p>

<p align="center">
  <b>EZMeal MSA 환경을 위한 트랜잭션 정합성 및 분산 보안 인프라</b><br>
  <span style="color:#56B16F">Transactional Outbox · 멱등성 보장(Inbox) · 보안 문맥 전파 · 공통 응답 표준화</span>
</p>

<br>

## ✨ 프로젝트 소개 (About The Project)
> **EZMeal** 마이크로서비스 아키텍처(MSA) 구축을 위한 핵심 공통 라이브러리입니다. <br>
> 개별 서비스가 비즈니스 로직에만 집중할 수 있도록, 분산 환경의 고질적인 문제인 데이터 정합성과 보안 전파를 자동화합니다.

<br>

본 모듈은 **EZMeal 프로젝트**의 마이크로서비스 아키텍처(MSA) 환경에서 서비스 간 **데이터 정합성(Transactional Integrity)**, **분산 보안 문맥 전파(Security Propagation)**, 그리고 **공통 응답 표준화**를 처리하기 위한 핵심 라이브러리입니다.

단일 데이터베이스를 사용하는 모놀리식 구조와 달리, 서비스가 분리된 환경에서 발생할 수 있는 '분산 트랜잭션 문제'를 **Outbox/Inbox 패턴**을 통해 해결하며, 모든 마이크로서비스가 동일한 보안 및 예외 처리 정책을 따르도록 강제합니다.

<br>

## 🚀 주요 기능 (Key Features)

- 📤 **이벤트 발행 보장 (Transactional Outbox)**: 비즈니스 로직과 이벤트 발행을 하나의 DB 트랜잭션으로 묶어, 어떠한 장애 상황에서도 메시지 유실 없는 안정적인 전달을 보장합니다.
- 📥 **메시지 멱등성 보장 (Inbox)**: 동일한 이벤트가 중복 수신되더라도 비즈니스 로직이 한 번만 실행되도록 수신 이력을 관리합니다.
- 🔐 **보안 컨텍스트 자동 전파**: API Gateway로부터 전달된 사용자 정보(ID, Role, Email)를 Feign Client 및 Kafka 통신 시 자동으로 다음 서비스에 전파합니다.
- 🛡️ **통합 예외 핸들링**: 전 서비스 공통의 `ErrorCode` 체계를 구축하여, 클라이언트에게 일관된 에러 응답을 반환합니다.
- 🔄 **자동화된 감사(Auditing)**: 모든 데이터의 생성/수정/삭제 시점과 수행한 사용자 정보를 자동으로 추적하여 데이터 이력을 투명하게 관리합니다.

<br>

## 📂 프로젝트 구조 (Folder & File Structure)

```text
com.ezmeal.common
├── config
│   ├── AsyncConfig.java             # 비동기 실행(@Async) 활성화 및 설정
│   ├── AuditConfig.java             # JPA Auditing(생성자/수정자 자동 기록) 설정
│   ├── KafkaConsumerConfig.java     # 카프카 컨슈머 및 역직렬화 설정
│   └── KafkaProducerConfig.java     # 카프카 프로듀서 및 직렬화 설정
├── entity
│   └── BaseEntity.java              # 모든 엔티티의 공통 필드(생성/수정/삭제일 및 담당자)
├── exception
│   ├── CustomException.java         # 최상위 커스텀 예외 클래스
│   ├── ErrorCode.java               # 예외 코드 인터페이스 규격
│   └── handler
│       └── GlobalExceptionHandler.java # @RestControllerAdvice 전역 예외 처리
├── message
│   ├── DomainEvent.java             # 모든 도메인 이벤트 인터페이스 (직렬화 정보 포함)
│   ├── EventEnvelope.java           # 공통 이벤트 메시지 포맷 (메타데이터 + 페이로드)
│   ├── CommonKafkaEventPublisher.java # [핵심] Outbox 패턴 기반 이벤트 발행기
│   ├── inbox                        # [수신] 멱등성 보장 로직 (InboxProcessor 등)
│   └── outbox                       # [발행] 전송 보장 로직 (OutboxMessageProcessor 등)
├── response
│   └── CommonApiResponse.java       # 전 서비스 공통 응답 규격 (성공/실패)
└── security
    ├── filter
    │   └── AuthenticationFilter.java # HTTP 헤더 기반 인증 필터 (Gateway 연동)
    ├── interceptor
    │   ├── FeignClientInterceptor.java # 서비스 간 호출(Feign) 시 인증 정보 전파
    │   └── KafkaSecurityInterceptor.java # 카프카 메시지 수신 시 인증 정보 전파
    └── principal
        └── CustomUserPrincipal.java # SecurityContext에 저장될 유저 상세 정보
```

<br>

## 🔄 핵심 처리 흐름 (Process Flow)

### 📤 1. 이벤트 발행 흐름 (Transactional Outbox 패턴)
이벤트 발행과 비즈니스 로직을 하나의 DB 트랜잭션으로 묶어 **"메시지 발행 보장"**을 실현합니다.

```text
[하위 애플리케이션]
 1️⃣ UserService.java (비즈니스 로직 수행 & 트랜잭션 시작)
       ⬇️ "eventPublisher.publish() 호출"

[공통 모듈]
 2️⃣ CommonKafkaEventPublisher.java (카프카로 안 쏘고 DB 저장 & 내부 알람)
       ┣━━ 💾[DB Insert] ➡️ OutboxMessage.java & OutboxRepository.java (상태: INIT)
       ┗━━ 📢 [내부 알람] ➡️ OutboxInternalEvent.java (JVM 메모리에 이벤트 던짐)

       ⬇️ (기다림... 하위 애플리케이션의 DB 트랜잭션이 성공적으로 Commit 됨)

 3️⃣ OutboxCommitListener.java (Commit 완료 순간 내부 이벤트 수신)
       ⬇️ "비동기 스레드에서 전송기(Processor) 호출"

 4️⃣ OutboxMessageProcessor.java (순서 검사 및 실제 카프카 전송)
       ┣━━ 🔍 [순서 검사] ➡️ 앞서 밀린 메시지(INIT 상태)가 있는지 DB 확인
       ┃
       ┣━━ ✅ [정상 발송] ➡️ 카프카 전송 ➡️ DB 상태 'PUBLISHED'로 업데이트 (완료!)
       ┃
       ┗━━ ❌[발송 실패 or 순서 밀림] ➡️ 종료 (상태는 'INIT' 유지)
                 ⬇️
                 🔄 [구원 투수 등장]
 5️⃣ OutboxRelay.java (10초마다 도는 패자부활전 스케줄러)
                 ┗━━ 🔍 DB에서 오래된 'INIT' 메시지들을 오름차순으로 조회
                 ┗━━ ➡️ 다시 4️⃣ OutboxMessageProcessor.java 에게 던져서 재시도
```

<br>

### 📥 2. 이벤트 수신 흐름 (Inbox 패턴 & 멱등성 보장)
중복 수신 시에도 비즈니스 로직이 한 번만 실행되도록 **"멱등성"**을 보장합니다.

```text
☁️ 메시지 도착 (네트워크 재시도로 인해 같은 메시지가 2번 올 수도 있음)
       ⬇️[하위 애플리케이션]
 1️⃣ ReviewEventListenerImpl.java (@KafkaListener 로 메시지 수신)
       ⬇️ "inboxProcessor.processOnce(eventId, 람다식) 호출"[공통 모듈]
 2️⃣ InboxProcessor.java (트랜잭션 시작)
       ┃
       ┣━━ 🔍 3️⃣ InboxRepository.java (DB에 이 eventId가 있는지 검사)
       ┃       ┃
       ┃       ┣━ 🛑 [이미 존재함] ➡️ 중복 수신, 아무 일도 안 하고 return (방어 성공)
       ┃       ┃
       ┃       ┗━ 🟢[처음 보는 경우] ➡️ 4️⃣ InboxMessage.java 엔티티 생성
       ┃                                  ┗━ 💾 DB에 수신 이력 Insert (저장)
       ⬇️[하위 애플리케이션]
 5️⃣ 람다식(businessLogic) 실행 (진짜 비즈니스 로직 수행, 예: 리뷰 삭제)
       ⬇️

 6️⃣ 트랜잭션 종료 (Commit)
       ┗━━ 🎯 비즈니스 데이터(리뷰 삭제) + Inbox 데이터(수신 이력)가 DB에 동시에 완벽하게 저장됨
       (만약 5️⃣번 로직에서 에러가 났다면? ➡️ 둘 다 롤백되므로 Inbox에도 기록이 안 남음)
```

<br>

---

## 💻 사용 예시 (Usage Examples)

실제 **Review 도메인**에 적용된 코드를 바탕으로 공통 모듈 사용법을 안내합니다.

### 1️⃣ 이벤트 발행 세팅 (Producer)
> 공통 모듈의 `CommonKafkaEventPublisher`를 주입받아 사용합니다.
```java
@Component
@RequiredArgsConstructor
public class ReviewEventProducerImpl implements ReviewEventProducer {

    // 공통 모듈의 핵심 이벤트 퍼블리셔 주입
    private final CommonKafkaEventPublisher eventPublisher;

    @Override
    public void publishUpdatedEvent(ReviewUpdatedEvent event) {
        eventPublisher.publish(
                "review.updated",      // topic
                event.reviewId(),      // aggregateId (동일 리뷰에 대한 순서 꼬임 방지용 키)
                "REVIEW_UPDATED",      // eventType
                event                  // payload (DomainEvent 구현체)
        );
    }
}
```

### 2️⃣ 서비스 레이어 (Service)
> 핵심: 하나의 서비스 클래스 내에서 **이벤트 발행(Outbox 적용)** 로직과 타 서버 이벤트 **수신 시 처리할(Inbox 적용)** 비즈니스 로직을 모두 관리합니다.
```java
@Slf4j
@Service
@RequiredArgsConstructor
public class ReviewService {

    private final ReviewRepository reviewRepository;
    private final ReviewEventProducer eventProducer;

    // =========================================================
    // [발행 측] 단건 리뷰 수정 및 이벤트 발행 (Outbox 패턴 적용)
    // =========================================================
    // 핵심: @Transactional이 선언되어 있으므로, 
    // 리뷰 데이터 변경과 Outbox DB 저장이 완벽하게 "하나의 트랜잭션"으로 묶입니다.
    @Transactional
    public ReviewResponse updateReview(ReviewUpdateCommand command) {
        // 1. 순수 비즈니스 로직 (리뷰 데이터 DB 수정)
        Review review = reviewRepository.findActiveById(command.reviewId())
                .orElseThrow(() -> new NotFoundException(ReviewErrorCode.REVIEW_NOT_FOUND));

        review.updateReview(command.userId(), command.role(), command.score(), command.contents());

        // 2. 이벤트 발행 (주의: 카프카로 바로 안 날아가고 Outbox 테이블에 안전하게 보관됨!)
        eventProducer.publishUpdatedEvent(ReviewUpdatedEvent.from(review));

        return ReviewResponse.from(review);
    }

    // =========================================================
    // [수신 측] 타 도메인(유저 탈퇴) 이벤트 발생 시 실행될 비즈니스 로직
    // =========================================================
    // 이 메서드는 아래 EventListener의 inboxProcessor.processOnce()
    // 내부에서 호출되므로, 이미 안전한 트랜잭션과 중복 방어망 안에서 실행됩니다.
    @Transactional
    public void bulkSoftDeleteByUserId(String userId, String deletedBy) {
        reviewRepository.bulkSoftDeleteByUserId(userId, deletedBy);
        log.info("유저({}) 탈퇴로 인해 작성한 모든 리뷰가 삭제 처리되었습니다. (deletedBy={})", userId, deletedBy);
        
        // 정상 종료되면, 이 비즈니스 데이터 변경과 Inbox 수신 기록이 DB에 동시 Commit 됩니다.
    }
}
```

### 3️⃣ 이벤트 수신 (Listener)
> 외부 이벤트를 수신할 때, 중복 실행 방지를 위해 **반드시 `inboxProcessor.processOnce()`를 사용**하여 서비스 로직을 감싸주어야 합니다.
```java
@Component
@RequiredArgsConstructor
public class ReviewEventListenerImpl {

    private final ReviewService reviewService;
    
    // 공통 모듈의 중복 방어를 위한 InboxProcessor 주입
    private final InboxProcessor inboxProcessor;

    @KafkaListener(topics = "user.deleted", groupId = "review-group")
    public void consumeUserDeletedEvent(EventEnvelope<UserDeletedMessage> envelope) {
        
        // 핵심 방어막: 카프카가 네트워크 이슈로 같은 메시지를 2~3번 보내도, 이 람다식 블록 안은 단 1번만 실행됨
        inboxProcessor.processOnce(envelope.eventId(), () -> {
            
            // 공통 모듈의 SecurityInterceptor가 세팅해둔 유저 정보 추출
            CustomUserPrincipal principal = getCurrentPrincipal();
            
            // 실제 데이터(payload)를 꺼내서 Service의 수신 측 비즈니스 로직 호출
            reviewService.bulkSoftDeleteByUserId(envelope.payload().userId(), principal.getUserId());
            
        });
    }
    
    private CustomUserPrincipal getCurrentPrincipal() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.isAuthenticated() && auth.getPrincipal() instanceof CustomUserPrincipal principal) {
            return principal;
        }
        return new CustomUserPrincipal("SYSTEM", Role.ADMIN, "system@ezmeal.com");
    }
}
```

<br>

## 🛠️ 메서드 중심 상세 설명 (Core Logic)

### 📡 1. 메시징 및 트랜잭션 정합성
*   **`CommonKafkaEventPublisher.publish()`**: 하위 서비스에서 이벤트를 발행할 때 사용하는 통합 엔트리 포인트입니다.
*   **`OutboxMessageProcessor.process()`**: 실제 카프카 전송을 수행하며, 발행 상태를 관리합니다. `REQUIRES_NEW` 트랜잭션을 사용하여 발행 이력을 개별적으로 커밋합니다. (순서 검사 포함)
*   **`InboxProcessor.processOnce()`**: 메시지 중복 수신 방지(멱등성)를 위해 하위 서비스의 비즈니스 로직을 감싸서 실행합니다. 중복 체크와 비즈니스 로직이 하나의 트랜잭션으로 묶입니다.
*   **`OutboxRelay.publishPendingMessages()`**: 10초마다 작동하는 스케줄러로, 예외 상황으로 인해 `INIT` 상태로 방치된 메시지를 구출합니다.

### 🔐 2. 분산 보안 및 문맥 전파
*   **`AuthenticationFilter`**: API 게이트웨이를 통과하여 유입된 요청 헤더에서 사용자 정보를 추출해 보안 문맥(`SecurityContext`)을 생성합니다.
*   **`FeignClientInterceptor`**: 서비스 간 내부 호출(Feign) 시, 현재 스레드에 저장된 인증 정보를 다음 서비스의 헤더로 전파합니다.
*   **`KafkaSecurityInterceptor`**: 카프카 메시지 소비 시 헤더에 포함된 유저 정보를 컨슈머 스레드로 복구하며, 스레드 재사용으로 인한 보안 정보 누수(Thread Leak)를 방지하기 위해 `clearContext()`를 호출합니다.

<br>

## ⚙️ 5. 주요 설정 가이드 (Configuration)
*   **`AsyncConfig`**: `@EnableAsync`를 통해 `OutboxCommitListener` 등에서 비동기 처리가 가능하도록 활성화합니다.
*   **`KafkaConfig`**: Zipkin 연동을 위해 `ObservationEnabled(true)`를 설정하고, `StringJsonMessageConverter`를 등록해 `@KafkaListener`에서 JSON을 DTO로 자동 매핑합니다.
*   **`AuditConfig`**: JPA Auditing을 통해 DB의 `createdBy`와 `modifiedBy`를 현재 인증된 유저(SecurityContext)로 자동 기록합니다.

---

## 💡 개발자 유의사항 및 빌드 안내

1. **트랜잭션 필수**: `CommonKafkaEventPublisher.publish()`를 호출하는 비즈니스 메서드는 반드시 `@Transactional`이 선언되어 있어야 합니다.
2. **DomainEvent 구현**: 모든 카프카 페이로드 DTO는 다형성 기반 직렬화를 위해 `DomainEvent` 인터페이스를 구현(implements)해야 합니다.
3. **Inbox 패턴 적용**: 이벤트를 수신하는 부분에서는 기존(0.9.0 버전) 방식 대신, 예시처럼 `InboxProcessor`를 주입받아 **`processOnce()` 내부에서 비즈니스 로직을 호출**하시길 바랍니다.
4. **BaseEntity 상속**: 모든 JPA 엔티티는 생성/수정 추적을 위해 `BaseEntity`를 상속받을 것을 권장합니다.

### 📦 빌드 방법
Gradle 탭 혹은 터미널을 열고 아래의 순서대로 Clean 후 Build를 진행해 주세요.
```bash
./gradlew clean build
```

---

### 🔗 깃허브 참고
본 기능에 대한 상세 구현 코드는 예시를 참고해 주세요. <br>
👉 **[2ZMeal/Review_server](https://github.com/2ZMeal/Review_server)** <br>
👉 **[2ZMeal/Customer_server](https://github.com/2ZMeal/Customer_server)**

---
