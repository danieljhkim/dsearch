package com.danieljhkim.dsearch.gateway.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/** Protects operational Actuator data on the gateway's public HTTP listener. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public final class ActuatorAuthFilter extends OncePerRequestFilter {

    private final String token;

    public ActuatorAuthFilter(@Value("${dsearch.admin.token:}") String token) {
        this.token = token == null ? "" : token;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getServletPath();
        return !path.equals("/actuator") && !path.startsWith("/actuator/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String path = request.getServletPath();
        if (path.equals("/actuator/health")
                || path.equals("/actuator/health/liveness")
                || path.equals("/actuator/health/readiness")) {
            chain.doFilter(request, response);
            return;
        }

        if (token.isBlank()) {
            response.sendError(HttpStatus.SERVICE_UNAVAILABLE.value(), "Actuator token is not configured");
            return;
        }
        String authorization = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
            response.sendError(HttpStatus.UNAUTHORIZED.value(), "Actuator bearer token required");
            return;
        }
        String presented = authorization.substring("Bearer ".length());
        if (!MessageDigest.isEqual(
                presented.getBytes(StandardCharsets.UTF_8), token.getBytes(StandardCharsets.UTF_8))) {
            response.sendError(HttpStatus.FORBIDDEN.value(), "Invalid Actuator bearer token");
            return;
        }
        chain.doFilter(request, response);
    }
}
