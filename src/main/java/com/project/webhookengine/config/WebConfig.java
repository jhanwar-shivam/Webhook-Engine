package com.project.webhookengine.config;

import com.project.webhookengine.interceptor.InboundRateLimitInterceptor;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@RequiredArgsConstructor
public class WebConfig implements WebMvcConfigurer {
    private final InboundRateLimitInterceptor inboundRateLimitInterceptor;
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(inboundRateLimitInterceptor)
                .addPathPatterns("/api/v1/events/dispatch");
    }
}
