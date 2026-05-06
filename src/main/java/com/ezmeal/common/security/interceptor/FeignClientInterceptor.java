package com.ezmeal.common.security.interceptor;

import com.ezmeal.common.security.principal.CustomUserPrincipal;
import feign.RequestInterceptor;
import feign.RequestTemplate;
import org.slf4j.MDC;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

@Component
public class FeignClientInterceptor implements RequestInterceptor {

    @Override
    public void apply(RequestTemplate template) {
        // MDC에서 traceId를 가져와서 Http 헤더에 추가
        String traceId = MDC.get("traceId");
        if (traceId != null) {
            template.header("X-TraceId", traceId);
        }

        // 현재 스레드의 SecurityContext에서 유저 정보를 꺼내서 HTTP 헤더에 담기
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication != null && authentication.getPrincipal() instanceof CustomUserPrincipal principal) {
            template.header("X-User-Id", principal.getUserId());
            template.header("X-User-Roles", principal.getRole().name());
            template.header("X-User-Email", principal.getEmail());
        }
    }
}
