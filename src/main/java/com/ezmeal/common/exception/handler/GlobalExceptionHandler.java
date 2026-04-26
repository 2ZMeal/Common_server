package com.ezmeal.common.exception.handler;

import com.ezmeal.common.exception.CustomException;
import com.ezmeal.common.response.CommonApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    // CustomException을 상속하는 경우 (각 도메인에서 커스텀 한 경우 및 types에 해당하는 경우)
    @ExceptionHandler(CustomException.class)
    public ResponseEntity<CommonApiResponse<Void>> handleCustomException(CustomException exception) {
        log.error("CustomException 발생: {}", exception.getErrorCode().getMessage());

        return ResponseEntity
                .status(exception.getErrorCode().getStatus())
                .body(CommonApiResponse.error(
                        exception.getErrorCode().getCode(),
                        exception.getErrorCode().getMessage()
                ));
    }

    // 그 외의 예상치 못한 예외가 발생하는 경우
    @ExceptionHandler(Exception.class)
    public ResponseEntity<CommonApiResponse<Void>> unhandledException(Exception exception) {
        log.error("Unhandled Exception 발생: ", exception);

        return ResponseEntity
                .status(500)
                .body(CommonApiResponse.error("SERVER_ERROR_500", "예상치 못한 서버 에러가 발생했습니다."));
    }
}
