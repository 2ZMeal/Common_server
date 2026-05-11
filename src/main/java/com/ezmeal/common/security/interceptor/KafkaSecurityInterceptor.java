package com.ezmeal.common.security.interceptor;

import com.ezmeal.common.enums.Role;
import com.ezmeal.common.security.principal.CustomUserPrincipal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

@Slf4j
public class KafkaSecurityInterceptor<K, V> implements RecordInterceptor<K, V> {

    @Override
    public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {

        Header userIdHeader = record.headers().lastHeader("X-User-Id");
        Header roleHeader = record.headers().lastHeader("X-User-Roles");
        Header emailHeader = record.headers().lastHeader("X-User-Email");
        Header traceIdHeader = record.headers().lastHeader("X-Original-Trace-Id");

        // 원본 TraceId 복원
        if (traceIdHeader != null) {
            String originalTraceId = new String(traceIdHeader.value(), StandardCharsets.UTF_8);
            MDC.put("originalTraceId", originalTraceId);
        }

        // 사용자 정보가 헤더에 존재하는 경우
        if (userIdHeader != null && roleHeader != null && emailHeader != null) {
            try {
                String userId = new String(userIdHeader.value(), StandardCharsets.UTF_8);
                String roleStr = new String(roleHeader.value(), StandardCharsets.UTF_8);
                String emailStr = new String(emailHeader.value(), StandardCharsets.UTF_8);
                Role role = Role.valueOf(roleStr);

                List<GrantedAuthority> authorities = List.of(new SimpleGrantedAuthority("ROLE_" + role.name()));
                CustomUserPrincipal principal = new CustomUserPrincipal(userId, role, emailStr);

                Authentication authentication = new UsernamePasswordAuthenticationToken(principal, null, authorities);

                // Consumer 스레드의 SecurityContext에 유저 정보 셋팅
                SecurityContextHolder.getContext().setAuthentication(authentication);

            } catch (Exception e) {
                log.warn("Kafka 헤더 인증 정보 처리 중 에러 발생", e);
                SecurityContextHolder.clearContext();
            }
        }
        // 시스템으로 호출하여 사용자 정보가 없는 경우
        else {
            // 이전 인증 정보가 남아있을 수 있으므로 비워줌
            SecurityContextHolder.clearContext();
        }

        return record;
    }

    // 비즈니스 로직 처리가 성공적으로 끝난 후 SecurityContext 비우기(스레드 누수 방지)
    @Override
    public void success(ConsumerRecord<K, V> record, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        SecurityContextHolder.clearContext();
        MDC.remove("originalTraceId");
    }

    // 에러가 발생하는 경우에도 SecurityContext를 비움
    @Override
    public void failure(ConsumerRecord<K, V> record, Exception exception, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        SecurityContextHolder.clearContext();
        MDC.remove("originalTraceId");
    }
}
