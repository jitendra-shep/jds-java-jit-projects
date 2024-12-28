package com.jit.microservice.twitter.to.kafka.runner.impl;

import com.jit.microservice.config.TwitterToKafkaServiceConfig;
import com.jit.microservice.twitter.to.kafka.exception.TwitterToKafkaServiceException;
import com.jit.microservice.twitter.to.kafka.listener.TwitterKafkaStatusListener;
import com.jit.microservice.twitter.to.kafka.runner.StreamRunner;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.*;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockTwitterKafkaStreamRunner implements StreamRunner {
    private static final Logger LOG = LoggerFactory.getLogger(MockTwitterKafkaStreamRunner.class);
    private final TwitterToKafkaServiceConfig twitterToKafkaServiceConfig;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM = new Random();
    private static final String[] WORDS = new String[]
            {
                    "apple", "banana", "cherry", "date", "elderberry",
                    "fig", "grape", "grapefruit", "guava", "honeydew",
                    "kiwi", "lemon", "lime", "lychee", "mango",
                    "mandarin", "nectarine", "orange", "papaya", "peach",
                    "pear", "pineapple", "plum", "pomegranate", "raspberry",
                    "strawberry"
            };

    private static final String tweetAsRawJson = "{" +
            "\"created_at\": \"{0}\"," +
            "\"id\": \"{1}\"," +
            "\"text\": \"{2}\"," +
            "\"user\": {\"id\": \"{3}\"}" +
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    private TwitterStream twitterStream;

    public MockTwitterKafkaStreamRunner(TwitterToKafkaServiceConfig twitterToKafkaServiceConfig, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfig = twitterToKafkaServiceConfig;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterToKafkaServiceConfig.getTwitterKeywords().toArray(new String[0]);
        int minTweetsLength = twitterToKafkaServiceConfig.getMockMinTweets();
        int maxTweetsLength = twitterToKafkaServiceConfig.getMockMaxTweets();
        long sleepTimeMs = twitterToKafkaServiceConfig.getMockSleepMs();

        stimulateTwitterStream(keywords, minTweetsLength, maxTweetsLength, sleepTimeMs);
    }

    private void stimulateTwitterStream(String[] keywords, int minTweetsLength, int maxTweetsLength, long sleepTimeMs) throws TwitterException {
        Executors.newSingleThreadExecutor().submit(() -> {
            while (true) {
                String formatTweetAsRawJson = getFormattedTweets(keywords, minTweetsLength, maxTweetsLength);
                Status status = TwitterObjectFactory.createStatus(formatTweetAsRawJson);
                twitterKafkaStatusListener.onStatus(status);
                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException e) {
                    throw new TwitterToKafkaServiceException("Error While Sleeping for waiting new Status to Create");
                }
            }
        });
    }

    private String getFormattedTweets(String[] keywords, int minTweetsLength, int maxTweetsLength) {
        String[] params = new String[]
                {
                        ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                        String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                        getRandomTweetContent(keywords, minTweetsLength, maxTweetsLength),
                        String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
                };

        return formatTweetAsJsonWithParam(params);
    }

    private static String formatTweetAsJsonWithParam(String[] params) {
        String tweet = tweetAsRawJson;

        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetsLength, int maxTweetsLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetsLength - minTweetsLength) + minTweetsLength;

        return constructRandonTweet(keywords, tweetLength, tweet);
    }

    private static String constructRandonTweet(String[] keywords, int tweetLength, StringBuilder tweet) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }

    @PreDestroy
    public void shutDown() {
        if (twitterStream != null) {
            LOG.info("Closing twitter stream.... . .!");
            twitterStream.shutdown();
        }
    }

}
