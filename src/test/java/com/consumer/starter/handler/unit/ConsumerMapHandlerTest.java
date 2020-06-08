package com.consumer.starter.handler.unit;

import static org.junit.jupiter.api.Assertions.*;

import com.consumer.starter.domain.EventTestDomain;
import com.consumer.starter.exception.NotFoundConsumerException;
import com.consumer.starter.handler.ConsumerMapHandler;
import com.consumer.starter.handler.KafkaConsumerHandler;
import com.consumer.starter.util.ConsumerProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

public class ConsumerMapHandlerTest extends UtilConfiguration {

  @BeforeAll
  static void cleanMap() {
    ConsumerMapHandler.cleanAllConsumers();
  }

  @Test
  public void shouldMapsConsumer() {
    ConsumerMapHandler.mapsConsumer(TEST_TOPIC_NAME, buildConcurrentMessageListenerContainer());
    assertNotNull(ConsumerMapHandler.getConsumerByTopic(TEST_TOPIC_NAME));
  }

  @Test
  public void shouldGetConsumerByTopic() {
    startAndRunConsumer();
    assertTrue(ConsumerMapHandler.getConsumerByTopic(TEST_TOPIC_NAME).isRunning());
  }

  @Test
  public void shouldThrowNotFoundWhentryToFindConsumerByTopic() {
    assertThrows(
        NotFoundConsumerException.class, () -> ConsumerMapHandler.getConsumerByTopic("123"));
  }

  @Test
  public void shouldCheckifConsumerIsRunning() {
    startAndRunConsumer();
    assertTrue(ConsumerMapHandler.isConsumerRunning(TEST_TOPIC_NAME));
  }

  @Test
  public void shouldStopConsumer() {
    startAndRunConsumer();
    ConsumerMapHandler.stopConsumerByTopic(TEST_TOPIC_NAME);
    assertFalse(ConsumerMapHandler.isConsumerRunning(TEST_TOPIC_NAME));
  }

  @Test
  public void shouldCleanAllConsumers() {
    startAndRunConsumer();
    ConsumerMapHandler.cleanAllConsumers();
    assertFalse(ConsumerMapHandler.isConsumerRunning(TEST_TOPIC_NAME));
  }

  private ConcurrentMessageListenerContainer<String, String>
      buildConcurrentMessageListenerContainer() {
    return new ConcurrentMessageListenerContainer<>(
        new DefaultKafkaConsumerFactory<>(
            ConsumerProperties.buildConsumerProps("localhost:9092", "consumerDefault")),
        new ContainerProperties(TEST_TOPIC_NAME));
  }

  private void startAndRunConsumer() {
    new KafkaConsumerHandler(new ObjectMapper(), kafkaConsumerConfig())
        .addTopicConsumer(EventTestDomain.class, eventTestDomain -> {});
  }
}
