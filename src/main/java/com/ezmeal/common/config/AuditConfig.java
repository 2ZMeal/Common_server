package com.ezmeal.common.config;

import com.ezmeal.common.security.principal.CustomUserPrincipal;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

@Slf4j
@Configuration
@EnableJpaAuditing
public class AuditConfig {

    @Bean
    public AuditorAware<String> auditorAware() {
        return () -> {
            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

            if (authentication == null || !authentication.isAuthenticated()
                    || authentication.getPrincipal().equals("anonymousUser")) {
                return Optional.empty();
            }

            try {
                CustomUserPrincipal principal = (CustomUserPrincipal) authentication.getPrincipal();
                return Optional.of(principal.getUserId());
            } catch (Exception e) {
                log.warn("SecurityContext에서 userId 정보를 확인할 수 없습니다. System 동작으로 간주합니다.", e);
                return Optional.empty();
            }
        };
    }
}
