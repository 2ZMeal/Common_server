package com.ezmeal.common.exception.types;

import com.ezmeal.common.exception.CustomException;
import com.ezmeal.common.exception.ErrorCode;

public class ForbiddenException extends CustomException {
    public ForbiddenException(ErrorCode errorCode) {
        super(errorCode);
    }
}
