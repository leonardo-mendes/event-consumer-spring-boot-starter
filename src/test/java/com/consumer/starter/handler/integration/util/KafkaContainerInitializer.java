package com.consumer.starter.handler.integration.util;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ContextConfiguration(initializers = {KafkaContainerInitializer.Initializer.class})
public class KafkaContainerInitializer extends GenericContainer<KafkaContainerInitializer> {

  static KafkaContainer kafkaContainer = new KafkaContainer();

  public static class Initializer
      implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {

      startKafka();

      TestPropertyValues.of(
              "consumer.starter.kafka.address=localhost:" + kafkaContainer.getFirstMappedPort())
          .applyTo(configurableApplicationContext.getEnvironment());
    }
  }

  private static void startKafka() {
    kafkaContainer.start();
  }
}
