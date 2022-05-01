package tech.pm.entities.raw;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Event {
  String id;
  String name;
  String sport;
  String status;
  String start_time;
}
