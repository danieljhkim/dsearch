package com.danieljhkim.dsearch.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;

@SpringBootTest(
        classes = ActuatorAccessIntegrationTest.TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "dsearch.admin.token=integration-secret")
@AutoConfigureObservability
class ActuatorAccessIntegrationTest {

    @LocalServerPort
    private int port;

    private final HttpClient client = HttpClient.newHttpClient();

    @Test
    void protectsOperationalEndpointsAndKeepsProbesOpen() throws IOException, InterruptedException {
        for (String path : new String[] {"/actuator", "/actuator/info", "/actuator/metrics", "/actuator/prometheus"}) {
            assertThat(get(path, null).statusCode())
                    .as(path + " without token")
                    .isEqualTo(HttpStatus.UNAUTHORIZED.value());
            assertThat(get(path, "wrong").statusCode())
                    .as(path + " with wrong token")
                    .isEqualTo(HttpStatus.FORBIDDEN.value());
            assertThat(get(path, "integration-secret").statusCode())
                    .as(path + " with valid token")
                    .isEqualTo(HttpStatus.OK.value());
        }

        for (String path :
                new String[] {"/actuator/health", "/actuator/health/liveness", "/actuator/health/readiness"}) {
            assertThat(get(path, null).statusCode()).as(path + " without token").isEqualTo(HttpStatus.OK.value());
        }
    }

    private HttpResponse<String> get(String path, String token) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .GET();
        if (token != null) {
            request.header("Authorization", "Bearer " + token);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import(ActuatorAuthFilter.class)
    static class TestApplication {}
}
