package com.consumer.starter.configuration.json;

import com.consumer.starter.exception.DeseralizationEventException;
import com.consumer.starter.util.JsonAvroUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.avro.specific.SpecificRecordBase;

public class SpecificRecordJsonDeserializer<T extends SpecificRecordBase>
    extends JsonDeserializer<T> {

  private final Class<T> genericClass;

  public SpecificRecordJsonDeserializer(Class<T> genericClass) {
    this.genericClass = genericClass;
  }

  @Override
  public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

    if (genericClass == null) {
      throw new DeseralizationEventException(
          "Error to deserialize a SpecificRecordBase. Generic Class not found.");
    }

    JsonNode jn = p.getCodec().readTree(p);
    return JsonAvroUtils.convertJsonToGenericSpecificRecord(jn.asText(), genericClass);
  }
}
