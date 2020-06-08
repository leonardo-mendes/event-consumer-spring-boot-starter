package com.consumer.starter.configuration.property;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ConsumerCustomProperties {

  public final String KAFKA_ADDRESS =
      ConsumerCustomDefaultProperties.PROPERTY_PREFIX.concat(".kafka.address");
  public final String TOPIC_PREFIX =
      ConsumerCustomDefaultProperties.PROPERTY_PREFIX.concat(".topic.prefix");
  public final String POOL_TIMEOUT =
      ConsumerCustomDefaultProperties.PROPERTY_PREFIX.concat(".pool.timeout");
  public final String APPLICATION_ID =
      ConsumerCustomDefaultProperties.PROPERTY_PREFIX.concat(".application.id");
}
