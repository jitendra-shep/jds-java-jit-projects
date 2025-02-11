package com.jit.microservice.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Data
@Configuration
@ConfigurationProperties(prefix = "twitter-to-kafka-service")
public class TwitterToKafkaServiceConfig {
    private List<String> twitterKeywords;
    private Boolean enableMockTweets;
    private Integer mockMinTweets;
    private Integer mockMaxTweets;
    private Long mockSleepMs;
}
