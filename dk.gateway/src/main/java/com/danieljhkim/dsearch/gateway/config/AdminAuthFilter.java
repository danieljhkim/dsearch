package com.danieljhkim.dsearch.gateway.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

/** Requires a bearer token for administrative index schema and alias operations. */
public final class AdminAuthFilter extends OncePerRequestFilter {

    public static final String ACTOR_ATTRIBUTE = "dsearch.admin.actor";
    public static final String ADMIN_ACTOR = "admin";

    private final String token;

    public AdminAuthFilter(String token) {
        this.token = token == null ? "" : token;
    }

    // GatewayConfig registers this filter on the container-normalized servlet URL pattern.
    // A second check against the raw URI would skip encoded or matrix-parameter paths.
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        if (token.isBlank()) {
            writeError(response, HttpStatus.SERVICE_UNAVAILABLE, "admin token is not configured");
            return;
        }
        String authorization = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            writeError(response, HttpStatus.UNAUTHORIZED, "missing admin bearer token");
            return;
        }
        String presented = authorization.substring("Bearer ".length()).trim();
        if (!constantTimeEquals(presented, token)) {
            writeError(response, HttpStatus.FORBIDDEN, "invalid admin bearer token");
            return;
        }
        request.setAttribute(ACTOR_ATTRIBUTE, ADMIN_ACTOR);
        filterChain.doFilter(request, response);
    }

    private static boolean constantTimeEquals(String left, String right) {
        return MessageDigest.isEqual(left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeError(HttpServletResponse response, HttpStatus status, String message) throws IOException {
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter()
                .write("{\"timestamp\":\""
                        + Instant.now()
                        + "\",\"status\":"
                        + status.value()
                        + ",\"error\":\""
                        + status.getReasonPhrase()
                        + "\",\"message\":\""
                        + message
                        + "\",\"path\":\"/api/v1/admin\"}");
    }
}
