package com.consumer.starter.domain;

import static lombok.AccessLevel.NONE;

import com.consumer.starter.configuration.json.SpecificRecordJsonSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.LocalDateTime;
import java.util.UUID;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.avro.specific.SpecificRecordBase;

@Data
@NoArgsConstructor
public class GenericEvent<T extends SpecificRecordBase> {

  @Setter(NONE)
  private String id;

  @Setter(NONE)
  private LocalDateTime eventEmittedDate;

  private String name;

  private String version;

  private String eventEmitterId;

  @JsonSerialize(using = SpecificRecordJsonSerializer.class)
  private T eventBody;

  public boolean hasEventBody() {
    return getEventBody() != null;
  }

  @Builder
  public GenericEvent(String name, String version, String eventEmitterId, T eventBody) {
    this.id = UUID.randomUUID().toString();
    this.eventEmittedDate = LocalDateTime.now();
    this.name = name;
    this.version = version;
    this.eventEmitterId = eventEmitterId;
    this.eventBody = eventBody;
  }
}
