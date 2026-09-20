package com.project.webhookengine.interceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NonNull;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class InboundRateLimitInterceptor implements HandlerInterceptor {
    StringRedisTemplate redisTemplate;
    @Override
    public boolean preHandle(HttpServletRequest request, @NonNull HttpServletResponse response, @NonNull Object handler)
            throws Exception {
        UUID tenantId = (UUID) request.getAttribute("tenantId");
        Integer maxRequestsPerSecond = (Integer) request.getAttribute("maxRps");

        if (tenantId == null || maxRequestsPerSecond == null) {
            return true;
        }

        long currentSecond = Instant.now().toEpochMilli();
        String redisKey = "rate_limit:" + tenantId + ":" + currentSecond;

        Long currentRequestCount = redisTemplate.opsForValue().increment(redisKey);

        if (currentRequestCount != null && currentRequestCount == 1) {
            redisTemplate.expire(redisKey, Duration.ofSeconds(2));
        }

        if (currentRequestCount != null && currentRequestCount > maxRequestsPerSecond) {
            response.setStatus(429); // 429 Too Many Requests
            response.getWriter().write("Rate limit exceeded. Maximum RPS: " + maxRequestsPerSecond);
            return false;
        }

        return true;
    }
}
