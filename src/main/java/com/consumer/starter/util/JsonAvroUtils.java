package com.consumer.starter.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import lombok.experimental.UtilityClass;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import tech.allegro.schema.json2avro.converter.AvroConversionException;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

@UtilityClass
public class JsonAvroUtils {

  private final JsonAvroConverter CONVERTER = new JsonAvroConverter();

  public String convertSpecificRecordToJson(SpecificRecordBase specificRecord) {

    try {

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Encoder enc = EncoderFactory.get().jsonEncoder(specificRecord.getSchema(), out);

      DatumWriter<SpecificRecordBase> writer =
          new SpecificDatumWriter<>(specificRecord.getSchema());
      writer.write(specificRecord, enc);
      enc.flush();

      return out.toString();

    } catch (IOException e) {
      throw new AvroConversionException("Error to convert SpecificRecord to Json");
    }
  }

  public <T extends SpecificRecordBase> T convertJsonToGenericSpecificRecord(
      String json, Class<T> classToConvert) {
    try {
      Schema schema = classToConvert.getDeclaredConstructor().newInstance().getSchema();
      return CONVERTER.convertToSpecificRecord(json.getBytes(), classToConvert, schema);
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException e) {
      throw new AvroConversionException("Error to convert Json to SpecificRecord", e);
    }
  }
}
