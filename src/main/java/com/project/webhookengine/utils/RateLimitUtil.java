package com.project.webhookengine.utils;

import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class RateLimitUtil {

    private static final String OUTBOUND_KEY_PREFIX = "outbound_rate_limit:";
    private static final String INBOUND_KEY_PREFIX = "inbound_rate_limit:tenant:";

    private final StringRedisTemplate redisTemplate;
    private DefaultRedisScript<Long> tokenBucketScript;

    @PostConstruct
    public void init() {
        tokenBucketScript = new DefaultRedisScript<>();
        tokenBucketScript.setLocation(new ClassPathResource("token_bucket.lua"));
        tokenBucketScript.setResultType(Long.class);
    }

    public boolean checkOutboundRateLimit(String domain, int capacity, int refillRatePerSecond) {
        return tryAcquireToken(OUTBOUND_KEY_PREFIX + domain, capacity, refillRatePerSecond);
    }

    public boolean checkInboundRateLimit(UUID tenantId, int maxRequestsPerSecond) {
        if (maxRequestsPerSecond <= 0) {
            return true;
        }
        return tryAcquireToken(INBOUND_KEY_PREFIX + tenantId, maxRequestsPerSecond, maxRequestsPerSecond);
    }

    private boolean tryAcquireToken(String redisKey, int capacity, int refillRatePerSecond) {
        long currentTimestamp = Instant.now().getEpochSecond();

        Long result = redisTemplate.execute(
                tokenBucketScript,
                Collections.singletonList(redisKey),
                String.valueOf(capacity),
                String.valueOf(refillRatePerSecond),
                String.valueOf(currentTimestamp)
        );

        return result != null && result == 1L;
    }
}
