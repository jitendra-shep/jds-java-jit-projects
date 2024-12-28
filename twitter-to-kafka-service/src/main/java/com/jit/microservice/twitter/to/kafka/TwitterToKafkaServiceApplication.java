package com.jit.microservice.twitter.to.kafka;

import com.jit.microservice.twitter.to.kafka.config.TwitterToKafkaServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.util.Arrays;


@SpringBootApplication
public class TwitterToKafkaServiceApplication implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);
    private final TwitterToKafkaServiceConfig twitterToKafkaServiceConfig;

    public TwitterToKafkaServiceApplication(TwitterToKafkaServiceConfig twitterToKafkaServiceConfig) {
        this.twitterToKafkaServiceConfig = twitterToKafkaServiceConfig;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("Twitter to Kafka Service Application Starts .... . ");
        LOG.info(Arrays.toString(twitterToKafkaServiceConfig.getTwitterKeywords().toArray(new String[] {})));
    }
}
