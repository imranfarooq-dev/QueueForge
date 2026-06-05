package com.queueforge.web;

import com.queueforge.job.InvalidJobStateException;
import com.queueforge.job.JobNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.List;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<ApiError> notFound(JobNotFoundException ex, HttpServletRequest req) {
        return build(HttpStatus.NOT_FOUND, ex.getMessage(), req, null);
    }

    @ExceptionHandler(InvalidJobStateException.class)
    public ResponseEntity<ApiError> invalidState(InvalidJobStateException ex, HttpServletRequest req) {
        return build(HttpStatus.CONFLICT, ex.getMessage(), req, null);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> validation(MethodArgumentNotValidException ex, HttpServletRequest req) {
        List<String> details = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .toList();
        return build(HttpStatus.BAD_REQUEST, "Validation failed", req, details);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiError> typeMismatch(MethodArgumentTypeMismatchException ex, HttpServletRequest req) {
        String msg = "Invalid value for parameter '" + ex.getName() + "': " + ex.getValue();
        return build(HttpStatus.BAD_REQUEST, msg, req, null);
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ApiError> missingParam(MissingServletRequestParameterException ex, HttpServletRequest req) {
        return build(HttpStatus.BAD_REQUEST, "Missing required parameter: " + ex.getParameterName(), req, null);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> unreadable(HttpMessageNotReadableException ex, HttpServletRequest req) {
        return build(HttpStatus.BAD_REQUEST, "Malformed or missing request body", req, null);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ApiError> methodNotAllowed(HttpRequestMethodNotSupportedException ex, HttpServletRequest req) {
        String msg = "Method " + ex.getMethod() + " not allowed for " + req.getRequestURI();
        return build(HttpStatus.METHOD_NOT_ALLOWED, msg, req, null);
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ApiError> noResource(NoResourceFoundException ex, HttpServletRequest req) {
        return build(HttpStatus.NOT_FOUND, "No endpoint at " + req.getRequestURI(), req, null);
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ApiError> dataIntegrity(DataIntegrityViolationException ex, HttpServletRequest req) {
        log.warn("Data integrity violation at {} {}: {}", req.getMethod(), req.getRequestURI(), ex.getMessage());
        return build(HttpStatus.CONFLICT, "Database constraint violation", req, null);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> generic(Exception ex, HttpServletRequest req) {
        log.error("Unhandled exception at {} {}", req.getMethod(), req.getRequestURI(), ex);
        String message = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
        Throwable cause = ex.getCause();
        List<String> details = cause != null && cause.getMessage() != null
                ? List.of(cause.getClass().getSimpleName() + ": " + cause.getMessage())
                : null;
        return build(HttpStatus.INTERNAL_SERVER_ERROR, message, req, details);
    }

    private ResponseEntity<ApiError> build(HttpStatus status, String message,
                                           HttpServletRequest req, List<String> details) {
        ApiError body = new ApiError(
                status.value(),
                status.getReasonPhrase(),
                message,
                req.getRequestURI(),
                details
        );
        return ResponseEntity.status(status).body(body);
    }
}
