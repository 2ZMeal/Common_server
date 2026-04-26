package com.ezmeal.common.exception.types;

import com.ezmeal.common.exception.CustomException;
import com.ezmeal.common.exception.ErrorCode;

public class BadRequestException extends CustomException {
    public BadRequestException(ErrorCode errorCode) {
        super(errorCode);
    }
}
