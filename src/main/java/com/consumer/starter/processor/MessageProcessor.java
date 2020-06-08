package com.consumer.starter.processor;

import com.consumer.starter.domain.GenericEvent;
import org.apache.avro.specific.SpecificRecordBase;

public interface MessageProcessor<T extends SpecificRecordBase> {

  void processMessage(GenericEvent<T> event);
}
