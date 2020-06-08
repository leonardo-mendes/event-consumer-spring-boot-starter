package com.consumer.starter.handler;

import static com.consumer.starter.configuration.property.ConsumerCustomProperties.*;
import static org.springframework.kafka.listener.ContainerProperties.AckMode.RECORD;

import com.consumer.starter.configuration.property.ConsumerConfiguration;
import com.consumer.starter.configuration.property.ConsumerCustomDefaultProperties;
import com.consumer.starter.listener.CustomMessageListener;
import com.consumer.starter.parameter.KafkaConsumerParameter;
import com.consumer.starter.processor.MessageProcessor;
import com.consumer.starter.util.ConsumerProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.stereotype.Service;

@Slf4j
@Service("KafkaConsumerHandler")
@RequiredArgsConstructor
public class KafkaConsumerHandler {

  private final ObjectMapper objectMapper;
  private final ConsumerConfiguration configurationProperties;

  public <T extends SpecificRecordBase> void addTopicConsumer(
      Class<T> eventClass, MessageProcessor<T> messageProcessor) {
    startOrCreateConsumer(buildParameters(eventClass, messageProcessor));
  }

  private <T extends SpecificRecordBase> KafkaConsumerParameter buildParameters(
      Class<T> eventClass, MessageProcessor<T> messageProcessor) {

    return KafkaConsumerParameter.builder()
        .topicName(
            configurationProperties
                .getProperty(TOPIC_PREFIX)
                .concat(eventClass.getSimpleName().toLowerCase()))
        .concurrency(1)
        .consumerProperties(
            ConsumerProperties.buildConsumerProps(
                getProperty(KAFKA_ADDRESS), getProperty(APPLICATION_ID)))
        .messageListener(new CustomMessageListener<>(messageProcessor, objectMapper, eventClass))
        .build();
  }

  private void startOrCreateConsumer(final KafkaConsumerParameter consumerParameter) {
    if (!ConsumerMapHandler.isConsumerRunning(consumerParameter.getTopicName())) {
      buildAndRunConsumer(consumerParameter);
    }
  }

  private void buildAndRunConsumer(final KafkaConsumerParameter consumerBuilder) {
    ContainerProperties containerProps = new ContainerProperties(consumerBuilder.getTopicName());
    containerProps.setPollTimeout(
        Integer.parseInt(configurationProperties.getProperty(POOL_TIMEOUT)));
    containerProps.setAckMode(RECORD);

    ConcurrentMessageListenerContainer<String, String> container =
        buildContainer(consumerBuilder, containerProps);
    container.start();

    ConsumerMapHandler.mapsConsumer(consumerBuilder.getTopicName(), container);
    log.info("Created and started kafka consumer for topic: " + consumerBuilder.getTopicName());
  }

  private ConcurrentMessageListenerContainer<String, String> buildContainer(
      final KafkaConsumerParameter consumerBuilder, final ContainerProperties containerProps) {

    ConsumerFactory<String, String> factory =
        new DefaultKafkaConsumerFactory<>(consumerBuilder.getConsumerProperties());
    ConcurrentMessageListenerContainer<String, String> container =
        new ConcurrentMessageListenerContainer<>(factory, containerProps);
    container.setupMessageListener(consumerBuilder.getMessageListener());
    container.setConcurrency(consumerBuilder.getConcurrency());
    return container;
  }

  private String getProperty(String property) {
    ConsumerCustomDefaultProperties defaultValues = new ConsumerCustomDefaultProperties();
    String enviromentProperty = configurationProperties.getProperty(property);
    if (enviromentProperty.equalsIgnoreCase(defaultValues.getKafkaAddress())
        || enviromentProperty.equalsIgnoreCase(defaultValues.getApplicationId())) {
      log.warn(
          "Consumer property {} has not been defined, default value {}.",
          property,
          enviromentProperty);
    }
    return enviromentProperty;
  }
}
