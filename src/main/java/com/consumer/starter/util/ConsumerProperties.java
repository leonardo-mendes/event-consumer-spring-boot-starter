package com.consumer.starter.util;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

import java.util.HashMap;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.apache.kafka.common.serialization.StringDeserializer;

@UtilityClass
public class ConsumerProperties {

  public static Map<String, Object> buildConsumerProps(String broker, String group) {
    Map<String, Object> props = new HashMap<>();
    props.put(BOOTSTRAP_SERVERS_CONFIG, broker);
    props.put(GROUP_ID_CONFIG, "consumer_starter_".concat(group).concat("_group"));
    props.put(ENABLE_AUTO_COMMIT_CONFIG, true);
    props.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return props;
  }
}
