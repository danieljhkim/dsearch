package com.danieljhkim.dsearch.gateway.config;

import com.danieljhkim.dsearch.common.config.AppConfig;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.Semaphore;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.filter.OncePerRequestFilter;

/** Bounds API concurrency and reads request bodies into a strictly capped buffer. */
public final class RequestAdmissionFilter extends OncePerRequestFilter {

    private static final Set<String> BODY_METHODS = Set.of("POST", "PUT", "PATCH");

    private final int maxBodyBytes;
    private final int retryAfterMillis;
    private final Semaphore admission;

    public RequestAdmissionFilter(AppConfig.RequestLimitsConfig limits) {
        this.maxBodyBytes = Math.max(1, limits.getMaxHttpBodyBytes());
        this.retryAfterMillis = Math.max(1, limits.getRetryAfterMillis());
        this.admission = new Semaphore(Math.max(1, limits.getMaxConcurrentHttpRequests()), true);
    }

    // GatewayConfig registers this filter on the container-normalized servlet URL pattern.
    // A second check against the raw URI would skip encoded or matrix-parameter paths.
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        if (!admission.tryAcquire()) {
            response.setHeader(HttpHeaders.RETRY_AFTER, Long.toString(Math.max(1L, (retryAfterMillis + 999L) / 1000L)));
            writeError(
                    response,
                    HttpStatus.TOO_MANY_REQUESTS,
                    "HTTP request capacity exhausted; retry after " + retryAfterMillis + " ms",
                    request.getRequestURI());
            return;
        }

        try {
            HttpServletRequest admittedRequest = request;
            if (BODY_METHODS.contains(request.getMethod())) {
                long declaredLength = request.getContentLengthLong();
                if (declaredLength > maxBodyBytes) {
                    writeError(
                            response,
                            HttpStatus.PAYLOAD_TOO_LARGE,
                            "HTTP body bytes (" + declaredLength + ") exceeds maximum allowed (" + maxBodyBytes + ")",
                            request.getRequestURI());
                    return;
                }
                byte[] body = request.getInputStream().readNBytes(maxBodyBytes + 1);
                if (body.length > maxBodyBytes) {
                    writeError(
                            response,
                            HttpStatus.PAYLOAD_TOO_LARGE,
                            "HTTP body bytes exceeds maximum allowed (" + maxBodyBytes + ")",
                            request.getRequestURI());
                    return;
                }
                admittedRequest = new BufferedBodyRequest(request, body);
            }
            filterChain.doFilter(admittedRequest, response);
        } finally {
            admission.release();
        }
    }

    private static void writeError(HttpServletResponse response, HttpStatus status, String message, String requestPath)
            throws IOException {
        response.setStatus(status.value());
        response.setContentType("application/json");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.getWriter()
                .write("{\"timestamp\":\"" + Instant.now() + "\",\"status\":" + status.value()
                        + ",\"error\":\"" + status.getReasonPhrase() + "\",\"message\":\""
                        + jsonEscape(message) + "\",\"path\":\"" + jsonEscape(requestPath) + "\"}");
    }

    private static String jsonEscape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static final class BufferedBodyRequest extends HttpServletRequestWrapper {
        private final byte[] body;

        private BufferedBodyRequest(HttpServletRequest request, byte[] body) {
            super(request);
            this.body = body;
        }

        @Override
        public ServletInputStream getInputStream() {
            ByteArrayInputStream input = new ByteArrayInputStream(body);
            return new ServletInputStream() {
                @Override
                public boolean isFinished() {
                    return input.available() == 0;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(ReadListener readListener) {
                    throw new UnsupportedOperationException("Async body reads are not supported");
                }

                @Override
                public int read() {
                    return input.read();
                }
            };
        }

        @Override
        public BufferedReader getReader() {
            return new BufferedReader(new InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
        }
    }
}
