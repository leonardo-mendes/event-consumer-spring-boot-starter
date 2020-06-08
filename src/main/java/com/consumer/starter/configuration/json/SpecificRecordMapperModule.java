package com.consumer.starter.configuration.json;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.avro.specific.SpecificRecordBase;

public class SpecificRecordMapperModule<T extends SpecificRecordBase> extends SimpleModule {

  public SpecificRecordMapperModule(Class<T> clazz) {
    addDeserializer(SpecificRecordBase.class, new SpecificRecordJsonDeserializer<T>(clazz));
  }
}
