package com.ezmeal.common.exception.types;

import com.ezmeal.common.exception.CustomException;
import com.ezmeal.common.exception.ErrorCode;

public class InternalServerError extends CustomException {
    public InternalServerError(ErrorCode errorCode) {
        super(errorCode);
    }
}
