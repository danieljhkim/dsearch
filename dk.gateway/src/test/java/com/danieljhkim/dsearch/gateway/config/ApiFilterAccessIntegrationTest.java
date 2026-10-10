package com.danieljhkim.dsearch.gateway.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.danieljhkim.dsearch.common.config.AppConfig;
import com.danieljhkim.dsearch.common.grpc.NodeClientManager;
import com.danieljhkim.dsearch.gateway.api.AdminIndexController;
import com.danieljhkim.dsearch.gateway.api.dto.InspectSchemaResponseDto;
import com.danieljhkim.dsearch.gateway.service.GatewayAdminIndexService;
import com.danieljhkim.dsearch.proto.cluster.ClusterServiceGrpc;
import com.danieljhkim.dsearch.proto.cluster.GetReplicaRepairsResponse;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Exercises production filter registrations through real embedded Tomcat and Spring MVC routing. */
@SpringBootTest(
        classes = ApiFilterAccessIntegrationTest.TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApiFilterAccessIntegrationTest {

    @LocalServerPort
    private int port;

    @jakarta.annotation.Resource
    private GatewayAdminIndexService adminService;

    @jakarta.annotation.Resource(name = "clusterNodeClientManager")
    private NodeClientManager<ClusterServiceGrpc.ClusterServiceBlockingStub> coordinator;

    @jakarta.annotation.Resource
    private ProbeController probe;

    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeEach
    void resetHandlers() {
        reset(adminService, coordinator);
        probe.echoCalls.set(0);
        probe.holdCalls.set(0);
        probe.entered = new CountDownLatch(1);
        probe.release = new CountDownLatch(1);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "/api/v1/admin/repairs",
                "/api/v1/%61dmin/repairs",
                "/api/v1/admin;x=1/repairs",
                "/%61pi/v1/admin/repairs",
                "/api;x=1/v1/admin/repairs",
                "/api/%761/admin/repairs",
                "/api/v1;x=1/admin/repairs",
                "/api/v1/admin/%72epairs"
            })
    void adminRoutesRequireAuthenticationBeforeControllerDispatch(String path) throws Exception {
        assertThat(send(request(path).GET(), null).statusCode()).isEqualTo(401);
        assertThat(send(request(path).GET(), "wrong").statusCode()).isEqualTo(403);
        verifyNoInteractions(coordinator, adminService);

        var stub = mock(ClusterServiceGrpc.ClusterServiceBlockingStub.class);
        when(coordinator.nextClient()).thenReturn(stub);
        when(stub.withDeadlineAfter(anyLong(), eq(TimeUnit.SECONDS))).thenReturn(stub);
        when(stub.getReplicaRepairs(any())).thenReturn(GetReplicaRepairsResponse.getDefaultInstance());
        assertThat(send(request(path).GET(), "integration-secret").statusCode()).isEqualTo(200);
        verify(stub).getReplicaRepairs(any());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "/api/v1/admin/indexes/movies/schema",
                "/api/v1/%61dmin/indexes/movies/schema",
                "/api/v1/admin;x=1/indexes/movies/schema"
            })
    void auditOperationsReceiveOnlyTheAuthenticatedActor(String path) throws Exception {
        assertThat(send(request(path).GET(), null).statusCode()).isEqualTo(401);
        verifyNoInteractions(adminService);
        when(adminService.inspectSchema("movies", AdminAuthFilter.ADMIN_ACTOR))
                .thenReturn(new InspectSchemaResponseDto());
        assertThat(send(request(path).GET(), "integration-secret").statusCode()).isEqualTo(200);
        verify(adminService).inspectSchema("movies", AdminAuthFilter.ADMIN_ACTOR);
    }

    @ParameterizedTest
    @ValueSource(strings = {"/api/v1/echo", "/%61pi/v1/echo", "/api;x=1/v1/echo"})
    void bodyLimitAppliesToKnownAndChunkedLengthsAndReleasesThePermit(String path) throws Exception {
        assertThat(send(request(path).POST(HttpRequest.BodyPublishers.ofString("x".repeat(64))), null)
                        .statusCode())
                .isEqualTo(413);
        assertThat(send(
                                request(path)
                                        .POST(HttpRequest.BodyPublishers.ofInputStream(
                                                () -> new ByteArrayInputStream(new byte[64]))),
                                null)
                        .statusCode())
                .isEqualTo(413);
        assertThat(probe.echoCalls.get()).isZero();

        var admitted = send(request(path).POST(HttpRequest.BodyPublishers.ofString("small")), null);
        assertThat(admitted.statusCode()).isEqualTo(200);
        assertThat(admitted.body()).isEqualTo("small");
        assertThat(probe.echoCalls.get()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"/api/v1/hold", "/%61pi/v1/hold", "/api;x=1/v1/hold"})
    void concurrencyLimitAppliesAcrossAllApiPathForms(String path) throws Exception {
        var first = client.sendAsync(request(path).GET().build(), HttpResponse.BodyHandlers.ofString());
        try {
            assertThat(probe.entered.await(5, TimeUnit.SECONDS)).isTrue();
            for (String echo : new String[] {"/api/v1/echo", "/%61pi/v1/echo", "/api;x=1/v1/echo"}) {
                var rejected = send(request(echo).POST(HttpRequest.BodyPublishers.ofString("small")), null);
                assertThat(rejected.statusCode()).as(echo).isEqualTo(429);
                assertThat(rejected.headers().firstValue("Retry-After")).contains("1");
            }
            assertThat(probe.echoCalls.get()).isZero();
            assertThat(probe.holdCalls.get()).isEqualTo(1);
            assertThat(send(request("/health").GET(), null).statusCode()).isEqualTo(200);
        } finally {
            probe.release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
        }
        assertThat(send(request("/api/v1/echo").POST(HttpRequest.BodyPublishers.ofString("small")), null)
                        .statusCode())
                .isEqualTo(200);
    }

    @Test
    void nonApiRoutesRemainPublic() throws Exception {
        assertThat(send(request("/health").GET(), null).statusCode()).isEqualTo(200);
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(10));
    }

    private HttpResponse<String> send(HttpRequest.Builder request, String token) throws Exception {
        if (token != null) {
            request.header("Authorization", "Bearer " + token);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({AdminIndexController.class, ProbeController.class})
    static class TestApplication {
        @Bean
        FilterRegistrationBean<AdminAuthFilter> adminAuthFilter() {
            return new GatewayConfig().adminAuthFilter("integration-secret");
        }

        @Bean
        FilterRegistrationBean<RequestAdmissionFilter> requestAdmissionFilter() {
            AppConfig config = new AppConfig();
            AppConfig.RequestLimitsConfig limits = new AppConfig.RequestLimitsConfig();
            limits.setMaxHttpBodyBytes(16);
            limits.setMaxConcurrentHttpRequests(1);
            config.setRequestLimits(limits);
            return new GatewayConfig().requestAdmissionFilter(config);
        }

        @Bean
        GatewayAdminIndexService adminService() {
            return mock(GatewayAdminIndexService.class);
        }

        @Bean
        @SuppressWarnings("unchecked")
        NodeClientManager<ClusterServiceGrpc.ClusterServiceBlockingStub> clusterNodeClientManager() {
            return mock(NodeClientManager.class);
        }
    }

    @RestController
    static class ProbeController {
        final AtomicInteger echoCalls = new AtomicInteger();
        final AtomicInteger holdCalls = new AtomicInteger();
        volatile CountDownLatch entered = new CountDownLatch(1);
        volatile CountDownLatch release = new CountDownLatch(1);

        @PostMapping("/api/v1/echo")
        String echo(@RequestBody String body) {
            echoCalls.incrementAndGet();
            return body;
        }

        @GetMapping("/api/v1/hold")
        String hold() throws InterruptedException {
            holdCalls.incrementAndGet();
            entered.countDown();
            if (!release.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for test request release");
            }
            return "released";
        }

        @GetMapping("/health")
        String health() {
            return "healthy";
        }
    }
}
