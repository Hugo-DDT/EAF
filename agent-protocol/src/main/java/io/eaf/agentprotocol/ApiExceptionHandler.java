package io.eaf.agentprotocol;

import io.eaf.shared.EafException;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.http.converter.HttpMessageNotReadableException;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(EafException.class)
    ResponseEntity<Problem> eaf(EafException e, HttpServletRequest request) {
        return response(e.status(), e.code(), e.getMessage(), request, e.retryable());
    }

    // 关闭协议入口后，未注册的路径必须保持 404，不能被通用异常分支误报成 500。
    @ExceptionHandler(NoResourceFoundException.class)
    ResponseEntity<Problem> missing(NoResourceFoundException e, HttpServletRequest request) {
        return response(404, "NOT_FOUND", "请求资源不存在。", request);
    }

    // 请求体无法反序列化时也必须停在入口，不能把伪造字段或非法元数据变成 500。
    @ExceptionHandler({IllegalArgumentException.class, MissingRequestHeaderException.class, MethodArgumentNotValidException.class, HttpMessageNotReadableException.class})
    ResponseEntity<Problem> invalid(Exception e, HttpServletRequest request) { return response(400, "INVALID_REQUEST", "请求格式或参数无效。", request); }

    @ExceptionHandler(Exception.class)
    ResponseEntity<Problem> unexpected(Exception e, HttpServletRequest request) { return response(500, "INTERNAL_ERROR", "服务未能完成请求。", request); }

    private ResponseEntity<Problem> response(int status, String code, String detail, HttpServletRequest request) {
        return response(status, code, detail, request, false);
    }

    private ResponseEntity<Problem> response(int status, String code, String detail, HttpServletRequest request, boolean retryable) {
        var body = new Problem("urn:eaf:error:" + code.toLowerCase().replace('_', '-'), code, status, detail, request.getRequestURI(), request.getHeader("X-Trace-Id"), retryable);
        return ResponseEntity.status(HttpStatus.valueOf(status)).contentType(MediaType.valueOf("application/problem+json")).body(body);
    }
    record Problem(String type, String code, int status, String detail, String instance, String traceId, boolean retryable) { }
}
// 本文件负责实现 EAF 的 ApiExceptionHandler.java 相关代码。
