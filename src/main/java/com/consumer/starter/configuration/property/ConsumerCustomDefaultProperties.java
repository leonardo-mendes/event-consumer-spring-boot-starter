package com.consumer.starter.configuration.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = ConsumerCustomDefaultProperties.PROPERTY_PREFIX)
public class ConsumerCustomDefaultProperties {

  static final String PROPERTY_PREFIX = "consumer.starter";

  private String kafkaAddress = "localhost:9092";

  private String topicPrefix = "io_dynamic_";

  private Integer poolTimeout = 100;

  private String applicationId = "unknown";
}
