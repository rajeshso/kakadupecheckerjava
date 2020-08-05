package com.example.kakadupecheckerjava;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Discards duplicate records from the input stream.
 */
public class DuplicationTransformer<K, V, E> implements
    Transformer<K, V, KeyValue<K, V>> {

  private final Duration maintainDurationInDays;
  private final KeyValueMapper<K, V, E> idExtractor;
  private final String transaction_event_log;
  private ProcessorContext context;
  private final static String EMPTY_STRING = new String();
  private WindowStore<E, String> eventIdStore;

  /**
   * @param idExtractor extracts a unique identifier from a record by which we de-duplicate input
   *                    records
   */
  DuplicationTransformer(KeyValueMapper<K, V, E> idExtractor, String transaction_event_log,
      final Duration maintainDurationInDays) {
    this.idExtractor = idExtractor;
    this.transaction_event_log = transaction_event_log;
    this.maintainDurationInDays = maintainDurationInDays;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(final ProcessorContext context) {
    this.context = context;
    eventIdStore = (WindowStore<E, String>) context.getStateStore(transaction_event_log);
  }

  @Override
  public KeyValue<K, V> transform(final K recordKey, final V recordValue) {
    final E eventId = idExtractor.apply(recordKey, recordValue);
    if (eventId != null) {
      if (isDuplicate(eventId)) {
        // Discard the value.
        return KeyValue.pair(recordKey, null);
      } else {
        remember(eventId, context.timestamp());
        // Forward the record downstream as-is.
        return KeyValue.pair(recordKey, recordValue);
      }
    }
    // Forward the record downstream as-is.
    return KeyValue.pair(recordKey, recordValue);
  }

  private boolean isDuplicate(final E eventId) {
    //TODO: Find a easier way for date calculation. Explore LocalDate, use a kotlin tuple/pair to return instants
    Date currentDate = new Date(context.timestamp());
    Date oldDate = new Date(context.timestamp() - maintainDurationInDays.toMillis());
    Instant thenInstant = oldDate.toInstant();
    Instant thisInstant = currentDate.toInstant();
    return eventIdStore.fetch(eventId,thenInstant, thisInstant).hasNext();
  }

  private void remember(final E eventId, final long eventTimestamp) {
    eventIdStore.put(eventId, EMPTY_STRING, eventTimestamp);
  }

  @Override
  public void close() {
    // Note: The store should NOT be closed manually here via `eventIdStore.close()`!
    // The Kafka Streams API will automatically close stores when necessary.
  }

}
