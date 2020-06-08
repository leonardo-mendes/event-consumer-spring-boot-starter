package com.consumer.starter.configuration.json;

import com.consumer.starter.util.JsonAvroUtils;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import org.apache.avro.specific.SpecificRecordBase;

public class SpecificRecordJsonSerializer extends JsonSerializer<SpecificRecordBase> {
  @Override
  public void serialize(SpecificRecordBase value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    gen.writeString(JsonAvroUtils.convertSpecificRecordToJson(value));
  }
}
