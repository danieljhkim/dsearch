package com.danieljhkim.dsearch.gateway.config;

import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.springframework.boot.micrometer.metrics.autoconfigure.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    private static final String PARTITION_TAG = "partitionId";
    private static final String OVERFLOW_PARTITION = "__overflow__";
    private static final int MAX_PARTITION_TAG_VALUES = 100;
    private static final Pattern PARTITION_ID_PATTERN = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> metricsCommonTags() {
        return registry -> registry.config().commonTags("application", "dsearch-gateway");
    }

    @Bean
    public MeterRegistryCustomizer<MeterRegistry> metricsCardinality() {
        return registry -> {
            registry.config().meterFilter(boundedPartitionIdValues());
            registry.config()
                    .meterFilter(MeterFilter.maximumAllowableTags(
                            "dsearch.gateway.", PARTITION_TAG, MAX_PARTITION_TAG_VALUES, MeterFilter.deny()));
        };
    }

    @Bean
    public TimedAspect timedAspect(MeterRegistry meterRegistry) {
        return new TimedAspect(meterRegistry);
    }

    private MeterFilter boundedPartitionIdValues() {
        Set<String> retainedValues = ConcurrentHashMap.newKeySet();
        retainedValues.add(OVERFLOW_PARTITION);

        return new MeterFilter() {
            @Override
            public Meter.Id map(Meter.Id id) {
                if (!isGatewayLatency(id)) {
                    return id;
                }

                String partitionId = id.getTag(PARTITION_TAG);
                if (partitionId == null
                        || !PARTITION_ID_PATTERN.matcher(partitionId).matches()) {
                    return id.withTag(Tag.of(PARTITION_TAG, OVERFLOW_PARTITION));
                }
                if (retainedValues.contains(partitionId)) {
                    return id;
                }

                synchronized (retainedValues) {
                    if (retainedValues.size() >= MAX_PARTITION_TAG_VALUES) {
                        return id.withTag(Tag.of(PARTITION_TAG, OVERFLOW_PARTITION));
                    }
                    retainedValues.add(partitionId);
                }
                return id;
            }
        };
    }

    private boolean isGatewayLatency(Meter.Id id) {
        return id.getName().startsWith("dsearch.gateway.") && id.getName().endsWith(".latency");
    }
}
