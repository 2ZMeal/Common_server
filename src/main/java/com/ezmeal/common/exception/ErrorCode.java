package com.ezmeal.common.exception;

import org.springframework.http.HttpStatus;

public interface ErrorCode {
    String name();
    HttpStatus getStatus();
    String getCode();
    String getMessage();
}
