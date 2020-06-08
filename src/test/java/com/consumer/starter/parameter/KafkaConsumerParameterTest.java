package com.consumer.starter.parameter;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.junit.jupiter.api.Assertions.*;

import com.consumer.starter.domain.EventTestDomain;
import com.consumer.starter.exception.InvalidParameterException;
import com.consumer.starter.handler.unit.UtilConfiguration;
import com.consumer.starter.listener.CustomMessageListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class KafkaConsumerParameterTest extends UtilConfiguration {

  public static final String BROKER_ADRESS = "localhost";

  @Test
  public void shouldBuildParameter() {
    KafkaConsumerParameter parameter =
        KafkaConsumerParameter.builder()
            .topicName(TEST_TOPIC_NAME)
            .concurrency(1)
            .consumerProperties(buildConsumerProps())
            .messageListener(getMessageListener())
            .build();

    assertNotNull(parameter.getMessageListener());
    assertNotNull(parameter.getConsumerProperties());
    assertFalse(parameter.getConsumerProperties().isEmpty());
    assertNotEquals(0, parameter.getConcurrency());
    assertNotNull(parameter.getTopicName());
    assertNotEquals("", parameter.getTopicName());
  }

  @Test
  public void shouldBuildParameterNoConcurrencyProvided() {
    KafkaConsumerParameter parameter =
        KafkaConsumerParameter.builder()
            .topicName(TEST_TOPIC_NAME)
            .consumerProperties(buildConsumerProps())
            .messageListener(getMessageListener())
            .build();

    assertNotEquals(0, parameter.getConcurrency());
  }

  @Test
  public void shouldNotBuildParameterNoTopicName() {
    assertThrows(
        InvalidParameterException.class,
        () ->
            KafkaConsumerParameter.builder()
                .consumerProperties(buildConsumerProps())
                .messageListener(getMessageListener())
                .build());
  }

  @Test
  public void shouldNotBuildParameterNoConsumerProperties() {
    assertThrows(
        InvalidParameterException.class,
        () ->
            KafkaConsumerParameter.builder()
                .topicName(TEST_TOPIC_NAME)
                .messageListener(getMessageListener())
                .build());
  }

  @Test
  public void shouldNotBuildParameterEmptyConsumerProperties() {
    assertThrows(
        InvalidParameterException.class,
        () ->
            KafkaConsumerParameter.builder()
                .topicName(TEST_TOPIC_NAME)
                .consumerProperties(new HashMap<>())
                .messageListener(getMessageListener())
                .build());
  }

  @Test
  public void shouldNotBuildParameternoMessageListener() {
    assertThrows(
        InvalidParameterException.class,
        () ->
            KafkaConsumerParameter.builder()
                .topicName(TEST_TOPIC_NAME)
                .consumerProperties(buildConsumerProps())
                .build());
  }

  private CustomMessageListener<EventTestDomain> getMessageListener() {
    return new CustomMessageListener<EventTestDomain>(
        event -> {}, new ObjectMapper(), EventTestDomain.class);
  }

  private Map<String, Object> buildConsumerProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(BOOTSTRAP_SERVERS_CONFIG, BROKER_ADRESS);
    return props;
  }
}
