package com.project.webhookengine.utils;

import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URISyntaxException;

@Component
public class UrlDomainExtractor {

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
            return domain.substring(4);
        }
        return domain;
    }
}
