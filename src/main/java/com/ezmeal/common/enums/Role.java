package com.ezmeal.common.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum Role {
    ADMIN("마스터 관리자"),
    COMPANY("업체 관리자"),
    USER("일반 회원");

    private final String description;
}
