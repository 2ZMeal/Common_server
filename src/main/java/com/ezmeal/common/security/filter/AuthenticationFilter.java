package com.ezmeal.common.security.filter;

import com.ezmeal.common.enums.Role;
import com.ezmeal.common.security.principal.CustomUserPrincipal;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class AuthenticationFilter extends OncePerRequestFilter {

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        if (request.getRequestURI().startsWith("/api") || request.getRequestURI().startsWith("/internal")) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        String userIdHeader = request.getHeader("X-User-Id");
        String roleHeader = request.getHeader("X-User-Roles");
        String emailHeader = request.getHeader("X-User-Email");

        // 기존에 있는 내역이 있다면 삭제
        SecurityContextHolder.clearContext();

        if(userIdHeader != null && roleHeader != null && emailHeader != null) {
            try {
                String userId = userIdHeader;
                String emailStr = emailHeader;

                Role role = Role.valueOf(roleHeader);
                List<GrantedAuthority> authorities
                        = List.of(new SimpleGrantedAuthority("ROLE_" + role.name()));

                CustomUserPrincipal principal
                        = new CustomUserPrincipal(userId, role, emailStr);

                Authentication authentication
                        = new UsernamePasswordAuthenticationToken(
                        principal,
                        null,
                        authorities
                );

                SecurityContextHolder.getContext().setAuthentication(authentication);
            } catch (IllegalArgumentException e) {
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                return;
            }
        }
        filterChain.doFilter(request, response);

    }
}
