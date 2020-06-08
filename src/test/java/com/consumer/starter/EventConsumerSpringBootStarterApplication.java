package com.consumer.starter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableAutoConfiguration
@SpringBootApplication
public class EventConsumerSpringBootStarterApplication {

  public static void main(String[] args) {
    SpringApplication.run(EventConsumerSpringBootStarterApplication.class, args);
  }
}
