package com.consumer.starter.handler.integration;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

import com.consumer.starter.EventConsumerSpringBootStarterApplication;
import com.consumer.starter.domain.EventTestDomain;
import com.consumer.starter.domain.GenericEvent;
import com.consumer.starter.handler.ConsumerMapHandler;
import com.consumer.starter.handler.KafkaConsumerHandler;
import com.consumer.starter.handler.integration.util.KafkaContainerInitializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = {EventConsumerSpringBootStarterApplication.class})
@Slf4j
public class KafkaConsumerHandlerIntegration extends KafkaContainerInitializer {

  private final String TOPIC_NAME = "io_dynamic_eventtestdomain";

  @Autowired KafkaConsumerHandler kafkaConsumerHandler;

  @Autowired ObjectMapper objectMapper;

  @Value(value = "${consumer.starter.kafka.address}")
  protected String kafkaAddress;

  @Test
  public void shouldCreateAndRunConsumer() throws JsonProcessingException, InterruptedException {

    createTopic();

    kafkaConsumerHandler.addTopicConsumer(
        EventTestDomain.class,
        eventTestDomain -> {
          log.info("Received message: {}", eventTestDomain.toString());
        });

    Thread.sleep(1000);
    publishEventTest();
    Thread.sleep(3000);

    Assertions.assertTrue(ConsumerMapHandler.isConsumerRunning(TOPIC_NAME));
  }

  private void createTopic() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
    KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) AdminClient.create(configs);
    kafkaAdminClient.createTopics(
        Collections.singletonList(new NewTopic(TOPIC_NAME, 1, (short) 1)));
    log.info("Topic created");
  }

  private void publishEventTest() throws JsonProcessingException {
    Producer<String, String> producer = new KafkaProducer<>(buildProducerProperties());
    producer.send(new ProducerRecord<>(TOPIC_NAME, buildEventToPublish()));
    log.info("Sent Event");
  }

  private Properties buildProducerProperties() {
    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
    props.put(ACKS_CONFIG, "all");
    props.put(RETRIES_CONFIG, 0);
    props.put(BATCH_SIZE_CONFIG, 2);
    props.put(LINGER_MS_CONFIG, 1);
    props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return props;
  }

  private String buildEventToPublish() throws JsonProcessingException {

    EventTestDomain eventTestDomain = new EventTestDomain();
    eventTestDomain.setUsername("username");
    eventTestDomain.setTweet("assas");

    return objectMapper.writeValueAsString(
        GenericEvent.builder()
            .eventBody(eventTestDomain)
            .eventEmitterId("emitterIdTest")
            .name("nameTest")
            .version("versionTest")
            .build());
  }
}
