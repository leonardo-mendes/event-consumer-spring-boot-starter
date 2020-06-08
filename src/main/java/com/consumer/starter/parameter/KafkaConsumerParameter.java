package com.consumer.starter.parameter;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import com.consumer.starter.exception.InvalidParameterException;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.springframework.kafka.listener.MessageListener;

@Getter
@ToString
public class KafkaConsumerParameter {

  private final String topicName;
  private final int concurrency;
  final Map<String, Object> consumerProperties;
  private MessageListener messageListener;

  @Builder
  public KafkaConsumerParameter(
      String topicName,
      int concurrency,
      Map<String, Object> consumerProperties,
      MessageListener messageListener) {
    validateArguments(topicName, consumerProperties, messageListener);

    this.topicName = topicName;
    this.concurrency = concurrency == 0 ? 1 : concurrency;
    this.consumerProperties = consumerProperties;
    this.messageListener = messageListener;
  }

  private void validateArguments(
      final String topicName,
      Map<String, Object> consumerProperties,
      final MessageListener messageListener) {
    if (isEmpty(topicName)) {
      throw new InvalidParameterException("Topic name should be provided");
    }

    if (consumerProperties == null || consumerProperties.isEmpty()) {
      throw new InvalidParameterException("Consumer properties should be provided");
    }

    if (messageListener == null) {
      throw new InvalidParameterException("Message listener should be provided");
    }
  }
}
