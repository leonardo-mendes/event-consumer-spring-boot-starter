package com.consumer.starter.handler.unit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.consumer.starter.domain.EventTestDomain;
import com.consumer.starter.handler.ConsumerMapHandler;
import com.consumer.starter.handler.KafkaConsumerHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class KafkaConsumerHandlerTest extends UtilConfiguration {

  KafkaConsumerHandler genericEventKafkaConsumerHandler =
      new KafkaConsumerHandler(new ObjectMapper(), kafkaConsumerConfig());

  @Test
  public void shouldCreateAndRunConsumer() {
    genericEventKafkaConsumerHandler.addTopicConsumer(EventTestDomain.class, eventTestDomain -> {});
    assertTrue(ConsumerMapHandler.isConsumerRunning(TEST_TOPIC_NAME));
  }
}
