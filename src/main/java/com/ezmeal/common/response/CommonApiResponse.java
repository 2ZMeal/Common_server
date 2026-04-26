package com.ezmeal.common.response;

import lombok.Getter;

@Getter
public class CommonApiResponse<T> {

    private final String code;
    private final String message;
    private final T data;

    private CommonApiResponse(String code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    // 데이터를 포함하는 성공 응답
    public static <T> CommonApiResponse<T> success(T data) {
        return new CommonApiResponse<>("SUCCESS", "요청이 성공적으로 처리되었습니다.", data);
    }

    // 메시지를 지정하는 성공 응답
    public static <T> CommonApiResponse<T> success(String message, T data) {
        return new CommonApiResponse<>("SUCCESS", message, data);
    }

    // 데이터가 없는 성공 응답 (삭제 등)
    public static <T> CommonApiResponse<T> success() {
        return new CommonApiResponse<>("SUCCESS", "요청이 성공적으로 처리되었습니다.", null);
    }

    // 에러 응답 (에러에는 코드와 메시지만 있고 데이터는 없음)
    public static <T> CommonApiResponse<T> error(String code, String message) {
        return new CommonApiResponse<>(code, message, null);
    }
}
