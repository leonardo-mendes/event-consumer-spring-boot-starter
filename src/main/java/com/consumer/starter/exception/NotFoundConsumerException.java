package com.consumer.starter.exception;

public class NotFoundConsumerException extends RuntimeException {

  public NotFoundConsumerException(String topic) {
    super("Not found topic ".concat(topic));
  }
}
