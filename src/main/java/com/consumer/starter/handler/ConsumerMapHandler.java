package com.consumer.starter.handler;

import com.consumer.starter.exception.NotFoundConsumerException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

@Slf4j
@UtilityClass
public class ConsumerMapHandler {

  protected final Map<String, ConcurrentMessageListenerContainer<String, String>> consumersMap =
      new HashMap<>();

  public void mapsConsumer(
      String topic, ConcurrentMessageListenerContainer<String, String> container) {
    consumersMap.put(topic, container);
  }

  public ConcurrentMessageListenerContainer<String, String> getConsumerByTopic(String topic) {
    return Optional.ofNullable(consumersMap.get(topic))
        .orElseThrow(() -> new NotFoundConsumerException(topic));
  }

  public Boolean isConsumerRunning(String topic) {
    ConcurrentMessageListenerContainer<String, String> consumer = consumersMap.get(topic);
    return consumer == null ? Boolean.FALSE : consumer.isRunning();
  }

  public void stopConsumerByTopic(String topic) {
    getConsumerByTopic(topic).stop();
    log.info("Consumer stopped!!");
  }

  public void cleanAllConsumers() {
    consumersMap.forEach((topic, container) -> container.stop());
    consumersMap.clear();
  }
}
