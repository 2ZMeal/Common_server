package com.ezmeal.common.exception.types;

import com.ezmeal.common.exception.CustomException;
import com.ezmeal.common.exception.ErrorCode;

public class NotFoundException extends CustomException {
    public NotFoundException(ErrorCode errorCode){
        super(errorCode);
    }
}
