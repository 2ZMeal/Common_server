package com.ezmeal.common.exception.types;

import com.ezmeal.common.exception.CustomException;
import com.ezmeal.common.exception.ErrorCode;

public class UnauthorizedException extends CustomException {
    public UnauthorizedException(ErrorCode errorCode) {
        super(errorCode);
    }
}
