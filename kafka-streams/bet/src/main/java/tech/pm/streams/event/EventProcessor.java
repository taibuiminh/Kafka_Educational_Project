package tech.pm.streams.event;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import tech.pm.entities.raw.Event;

@Slf4j
public class EventProcessor implements Processor<String, Event, Void, Void> {

  private KeyValueStore<String, Event> state;

  @Override
  public void init(ProcessorContext<Void, Void> context) {
    Processor.super.init(context);
    this.state = context.getStateStore("event-state-store");
  }

  @Override
  public void process(Record<String, Event> record) {
    if (record.value() != null) {
      if (record.value().getStatus().equals("Active")) {//todo ask yura
        log.info("Event was created with key {} and value {}", record.key(), record.value());
        this.state.put(record.value().getId(), record.value());
      } else {
        log.info("Skipping record with non active status {}", record.value());
      }
    }
  }

  @Override
  public void close() {
    Processor.super.close();
  }
}
