package com.project.webhookengine.interceptor;

import com.project.webhookengine.utils.RateLimitUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class InboundRateLimitInterceptor implements HandlerInterceptor {

    private final RateLimitUtil rateLimitUtil;

    @Override
    public boolean preHandle(
            HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull Object handler
    ) throws Exception {
        UUID tenantId = (UUID) request.getAttribute("tenantId");
        Integer maxRequestsPerSecond = (Integer) request.getAttribute("maxRps");

        if (tenantId == null || maxRequestsPerSecond == null) {
            return true;
        }

        if (rateLimitUtil.checkInboundRateLimit(tenantId, maxRequestsPerSecond)) {
            return true;
        }

        response.setStatus(429);
        response.getWriter().write("Rate limit exceeded. Maximum RPS: " + maxRequestsPerSecond);
        return false;
    }
}
