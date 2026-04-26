package com.ezmeal.common.security.principal;

import com.ezmeal.common.enums.Role;
import lombok.Getter;

@Getter
public class CustomUserPrincipal {

    private final String userId;
    private final Role role;

    public CustomUserPrincipal(String userId, Role role) {
        this.userId = userId;
        this.role = role;
    }
}
