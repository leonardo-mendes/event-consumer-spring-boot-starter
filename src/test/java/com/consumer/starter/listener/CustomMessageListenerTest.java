package com.consumer.starter.listener;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.consumer.starter.domain.EventTestDomain;
import com.consumer.starter.domain.GenericEvent;
import com.consumer.starter.exception.DeseralizationEventException;
import com.consumer.starter.processor.MessageProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

public class CustomMessageListenerTest {

  @Test
  public void should_thrown_exception() throws JsonProcessingException {
    ObjectMapper objectMapper = mock(ObjectMapper.class);
    when(objectMapper.readValue(anyString(), eq(GenericEvent.class)))
        .thenThrow(JsonProcessingException.class);
    MessageProcessor<EventTestDomain> processor = mock(MessageProcessor.class);
    CustomMessageListener<EventTestDomain> listener =
        new CustomMessageListener<EventTestDomain>(processor, objectMapper, EventTestDomain.class);
    ConsumerRecord<String, String> record = getRecord();
    assertThrows(DeseralizationEventException.class, () -> listener.onMessage(record));
    verify(processor, times(0)).processMessage(any());
    verify(objectMapper, times(1)).readValue(anyString(), eq(GenericEvent.class));
  }

  private ConsumerRecord<String, String> getRecord() throws JsonProcessingException {
    return new ConsumerRecord<>(
        "topic",
        1,
        1,
        "key",
        "{\"id\":\"123\",\"name\":\"eventTest\",\"version\":\"2\",\"eventEmitterId\":\"123\",\"eventBody\": null }");
  }
}
