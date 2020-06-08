package com.consumer.starter.handler.unit;

import static com.consumer.starter.configuration.property.ConsumerCustomProperties.*;

import com.consumer.starter.configuration.property.ConsumerConfiguration;
import com.consumer.starter.configuration.property.ConsumerCustomDefaultProperties;

public class UtilConfiguration {

  protected static final String TEST_TOPIC_NAME = "io_dynamic_eventtestdomain";

  protected ConsumerConfiguration kafkaConsumerConfig() {

    ConsumerCustomDefaultProperties customProperties = new ConsumerCustomDefaultProperties();

    ConsumerConfiguration config = new ConsumerConfiguration();
    config.put(KAFKA_ADDRESS, customProperties.getKafkaAddress());
    config.put(TOPIC_PREFIX, customProperties.getTopicPrefix());
    config.put(POOL_TIMEOUT, String.valueOf(customProperties.getPoolTimeout()));
    config.put(APPLICATION_ID, customProperties.getApplicationId());
    return config;
  }
}
