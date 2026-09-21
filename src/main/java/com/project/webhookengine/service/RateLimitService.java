package com.project.webhookengine.service;

import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Collections;

@Service
@RequiredArgsConstructor
public class RateLimitService {
    private final StringRedisTemplate redisTemplate;
    private DefaultRedisScript<Long> tokenBucketScript;

    @PostConstruct
    public void init() {
        tokenBucketScript = new DefaultRedisScript<>();
        tokenBucketScript.setLocation(new ClassPathResource("token_bucket.lua"));
        tokenBucketScript.setResultType(Long.class);
    }

    public String extractDomain(String targetUrl) throws URISyntaxException {
        if (targetUrl == null || targetUrl.isBlank()) {
            throw new IllegalArgumentException("Target URL cannot be null or empty");
        }

        URI uri = new URI(targetUrl);
        String domain = uri.getHost();

        if (domain == null) {
            throw new URISyntaxException(targetUrl, "Could not extract domain");
        }

        if (domain.startsWith("www.")) {
            domain = domain.substring(4);
        }

        return domain;
    }

    public boolean checkOutboundRateLimit(String domain, int capacity, int refillRatePerSecond) {
        String redisKey = "outbound_rate_limit:" + domain;
        long currentTimestamp = Instant.now().getEpochSecond();

        Long result = redisTemplate.execute(
                tokenBucketScript,
                Collections.singletonList(redisKey), // KEYS[1]
                String.valueOf(capacity),            // ARGV[1]
                String.valueOf(refillRatePerSecond), // ARGV[2]
                String.valueOf(currentTimestamp)     // ARGV[3]
        );

        return (result == 1L);
    }
}