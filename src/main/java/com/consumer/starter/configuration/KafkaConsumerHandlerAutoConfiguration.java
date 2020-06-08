package com.consumer.starter.configuration;

import static com.consumer.starter.configuration.property.ConsumerCustomProperties.*;

import com.consumer.starter.configuration.property.ConsumerConfiguration;
import com.consumer.starter.configuration.property.ConsumerCustomDefaultProperties;
import com.consumer.starter.handler.KafkaConsumerHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
@RequiredArgsConstructor
@ConditionalOnClass(KafkaConsumerHandler.class)
@EnableConfigurationProperties(ConsumerCustomDefaultProperties.class)
public class KafkaConsumerHandlerAutoConfiguration {

  private final ConsumerCustomDefaultProperties properties;
  private final Environment environment;

  @Bean
  @ConditionalOnMissingBean
  public ConsumerConfiguration kafkaConsumerConfig() {

    String kafkaAddress =
        environment.getProperty(KAFKA_ADDRESS) != null
            ? environment.getProperty(KAFKA_ADDRESS)
            : properties.getKafkaAddress();
    String topicPrefix =
        environment.getProperty(TOPIC_PREFIX) != null
            ? environment.getProperty(TOPIC_PREFIX)
            : properties.getTopicPrefix();
    String poolTimeout =
        environment.getProperty(POOL_TIMEOUT) != null
            ? environment.getProperty(POOL_TIMEOUT)
            : String.valueOf(properties.getPoolTimeout());
    String applicationId =
        environment.getProperty(APPLICATION_ID) != null
            ? environment.getProperty(APPLICATION_ID)
            : properties.getApplicationId();

    ConsumerConfiguration config = new ConsumerConfiguration();
    config.put(KAFKA_ADDRESS, kafkaAddress);
    config.put(TOPIC_PREFIX, topicPrefix);
    config.put(POOL_TIMEOUT, poolTimeout);
    config.put(APPLICATION_ID, applicationId);
    return config;
  }

  @Bean
  @ConditionalOnMissingBean
  public KafkaConsumerHandler addConsumer() {
    return new KafkaConsumerHandler(customObjectMapper(), kafkaConsumerConfig());
  }

  @Bean
  public ObjectMapper customObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    return objectMapper;
  }
}
