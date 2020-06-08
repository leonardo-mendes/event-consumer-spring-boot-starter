package com.consumer.starter.listener;

import com.consumer.starter.configuration.json.SpecificRecordMapperModule;
import com.consumer.starter.domain.GenericEvent;
import com.consumer.starter.exception.DeseralizationEventException;
import com.consumer.starter.processor.MessageProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

@Slf4j
public class CustomMessageListener<T extends SpecificRecordBase>
    implements MessageListener<String, String> {

  private final MessageProcessor<T> messageProcessor;

  private final ObjectMapper objectMapper;

  public CustomMessageListener(
      MessageProcessor<T> messageProcessor, ObjectMapper objectMapper, Class<T> clazz) {
    this.messageProcessor = messageProcessor;
    this.objectMapper = objectMapper;
    this.registerMapperModule(clazz);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onMessage(ConsumerRecord<String, String> data) {
    log.info("Processing message: {}. ", data);
    try {
      messageProcessor.processMessage(objectMapper.readValue(data.value(), GenericEvent.class));
    } catch (JsonProcessingException e) {
      throw new DeseralizationEventException(e.getMessage());
    }
  }

  private void registerMapperModule(Class<T> clazz) {
    objectMapper.registerModule(new SpecificRecordMapperModule<>(clazz));
  }
}
