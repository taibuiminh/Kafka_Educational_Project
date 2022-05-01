package tech.pm.streams.event;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import tech.pm.entities.raw.Event;

public class EventProcessorSupplier implements ProcessorSupplier<String, Event, Void, Void> {
  @Override
  public Processor<String, Event, Void, Void> get() {
    return new EventProcessor();
  }
}
