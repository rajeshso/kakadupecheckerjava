package com.example.kakadupecheckerjava;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventDeduplication {

  private String applicationID;
  private Duration windowRetentionDuration;
  private String transactionLogName;
  private String inputTopic;
  private String outputTopic;
  private String outputDuplicateTopic;
  private String bootstrapServers;
  private final Logger logger = LoggerFactory.getLogger(EventDeduplication.class);


  public EventDeduplication(String applicationID, Duration windowRetentionDuration,
      String transactionLogName, String inputTopic, String outputTopic,
      String outputDuplicateTopic, String bootstrapServers) {
    this.applicationID = applicationID;
    this.windowRetentionDuration = windowRetentionDuration;
    this.transactionLogName = transactionLogName;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
    this.outputDuplicateTopic = outputDuplicateTopic;
    this.bootstrapServers = bootstrapServers;
  }

  public EventDeduplication() { //TODO: This is for dev testing
    this.applicationID = "event-deduplication";
    this.windowRetentionDuration = Duration.ofDays(1);
    this.transactionLogName = "eventId-store";
    inputTopic = "inputTopic";
    outputTopic = "outputTopic";
    outputDuplicateTopic = "outputDuplicateTopic";
    bootstrapServers = "127.0.0.1:9092";
  }

  public static void main(String[] args) {
    EventDeduplication eventDeduplication = new EventDeduplication();
    final String stateDirectoryPath = Paths.get("").toAbsolutePath().toString();
    Properties streamsConfiguration = eventDeduplication
        .getStreamsConfiguration(eventDeduplication.bootstrapServers, stateDirectoryPath);
    Topology builder = eventDeduplication
        .createTopology();

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    //print the topology
    eventDeduplication.logger.info(streams.toString());


    streams.start();
    //Add the shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

  public Topology createTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    //1 - Stream from kafka
    final KStream<String, String> inputStream = builder.stream(inputTopic, Consumed
        .with(Serdes.String(), Serdes.String()));

    //1 - Add a State store
    Map<String, String> topicConfig = new HashMap<>();//TODO: Find the topic config
    StoreBuilder<WindowStore<String, String>> storeBuilder = Stores
        .windowStoreBuilder(
            Stores.persistentTimestampedWindowStore(transactionLogName, windowRetentionDuration,
                windowRetentionDuration, true),
            Serdes.String(),
            Serdes.String())
        .withLoggingEnabled(topicConfig);
    builder.addStateStore(storeBuilder);

    //2 - Add a transformer to the inputstream
    final KStream<String, String> deduplicated = inputStream
        .transform(() -> new DuplicationTransformer<>(((key, value) -> key), transactionLogName,
                windowRetentionDuration),
            transactionLogName);
    //3 branch
    final KStream<String, String>[] deduplicatedBranches = deduplicated.branch(
        (key, value) -> value == null,
        (key, value) -> value != null
    );
    deduplicatedBranches[0].to(outputDuplicateTopic);
    deduplicatedBranches[1].to(outputTopic);

    //Bring all this together
    return builder.build();
  }


  public Properties getStreamsConfiguration(final String bootstrapServers,
      final String stateDirectoryPath) {
    Properties streamsConfiguration = new Properties();
    streamsConfiguration
        .put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID);
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsConfiguration
        .put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration
        .put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // The commit interval for flushing records to state stores and downstream must be lower than
    // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
    // TODO: Configurable value based on tuning in production system. Check with Lukasz/Arnaud for lateMessages issue
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    // TODO: Remove this in production. This is for testing
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG,
        stateDirectoryPath);
    return streamsConfiguration;
  }
}