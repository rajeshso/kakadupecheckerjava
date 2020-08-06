package com.example.kakadupecheckerjava;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest
//@DirtiesContext
@EmbeddedKafka(
topics = {"inputTopic","outputTopic","outputDuplicateTopic"}
)
@TestPropertySource(properties = {
    "spring.application.name.applicationID=applicationID",
    "spring.application.name.windowRetentionDuration=1",
    "spring.application.name.transactionLogName=testValue",
    "spring.application.name.inputTopic=inputTopic",
    "spring.application.name.outputTopic=outputTopic",
    "spring.application.name.outputDuplicateTopic=outputDuplicateTopic",
    "spring.application.name.bootstrapServers=localhost:9092"
})
@TestInstance(Lifecycle.PER_CLASS)
public class SimpleKafkaTest {

  private static final String INPUT_TOPIC = "inputTopic";

  private static final String OUTPUT_TOPIC = "outputTopic";

  private static final String OUTPUT_DUPLICATE_TOPIC = "outputDuplicateTopic";

  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @Autowired
  KafkaStreams kafkaStreams;

  BlockingQueue<ConsumerRecord<String, String>> records1;

  KafkaMessageListenerContainer<String, String> container1;

  BlockingQueue<ConsumerRecord<String, String>> records2;

  KafkaMessageListenerContainer<String, String> container2;

  BlockingQueue<ConsumerRecord<String, String>> records3;

  KafkaMessageListenerContainer<String, String> container3;


  @BeforeAll
  void setUp() {

    setupTopic1();
    //setupTopic2();
    //setupTopic3();
  }

  private void setupTopic1() {
    Map<String, Object> configs = new HashMap<>(
        KafkaTestUtils.consumerProps("applicationID", "false", embeddedKafkaBroker));
    DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new StringDeserializer());
    ContainerProperties containerProperties = new ContainerProperties(INPUT_TOPIC);
    container1 = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    records1 = new LinkedBlockingQueue<>();
    container1.setupMessageListener((MessageListener<String, String>) records1::add);
    container1.start();
    ContainerTestUtils.waitForAssignment(container1, embeddedKafkaBroker.getPartitionsPerTopic());
  }

  private void setupTopic2() {
    Map<String, Object> configs = new HashMap<>(
        KafkaTestUtils.consumerProps("applicationID", "false", embeddedKafkaBroker));
    DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new StringDeserializer());
    ContainerProperties containerProperties = new ContainerProperties(OUTPUT_TOPIC);
    container2 = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    records2 = new LinkedBlockingQueue<>();
    container2.setupMessageListener((MessageListener<String, String>) records2::add);
    container2.start();
    ContainerTestUtils.waitForAssignment(container2, embeddedKafkaBroker.getPartitionsPerTopic());
  }

  private void setupTopic3() {
    Map<String, Object> configs = new HashMap<>(
        KafkaTestUtils.consumerProps("applicationID", "false", embeddedKafkaBroker));
    DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new StringDeserializer());
    ContainerProperties containerProperties = new ContainerProperties(OUTPUT_DUPLICATE_TOPIC);
    container3 = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    records3 = new LinkedBlockingQueue<>();
    container3.setupMessageListener((MessageListener<String, String>) records3::add);
    container3.start();
    ContainerTestUtils.waitForAssignment(container3, embeddedKafkaBroker.getPartitionsPerTopic());
  }

  @AfterAll
  void tearDown() {
    container1.stop();
    //container2.stop();
    //container3.stop();
  }



  @Test
  public void kafkaSetup_withTopic_ensureSendMessageIsReceived() throws Exception {
    // Arrange
    System.out.println("kafkaStreams.state() = " + kafkaStreams.state());
    Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
    Producer<String, String> producer = new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new StringSerializer()).createProducer();

    // Act
    producer.send(new ProducerRecord<>(INPUT_TOPIC, "my-aggregate-id", "{\"event\":\"Test Event\"}"));
    producer.flush();

    try {
      Thread.sleep(300);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Assert
    ConsumerRecord<String, String> singleRecord = records1.poll(100, TimeUnit.MILLISECONDS);
    assertThat(singleRecord).isNotNull();
    assertThat(singleRecord.key()).isEqualTo("my-aggregate-id");
    assertThat(singleRecord.value()).isEqualTo("{\"event\":\"Test Event\"}");
  }

  @Disabled
  public void kafkaStreams_withTopic_ensureSendMessageIsReceived() throws Exception {
    // Arrange
    Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
    Producer<String, String> producer = new DefaultKafkaProducerFactory<>(configs, new StringSerializer(), new StringSerializer()).createProducer();

    // Act
    producer.send(new ProducerRecord<>(INPUT_TOPIC, "my-aggregate-id", "{\"event\":\"Test Event\"}"));
    producer.flush();

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Assert
    ConsumerRecord<String, String> singleRecord = records2.poll(100, TimeUnit.MILLISECONDS);
    assertThat(singleRecord).isNotNull();
    assertThat(singleRecord.key()).isEqualTo("my-aggregate-id");
    assertThat(singleRecord.value()).isEqualTo("{\"event\":\"Test Event\"}");
  }


  @Test
  void contextLoads() {
    assert(true);
  }

  @Disabled
  public void testReceivingKafkaEvents() {
    System.out.println("123");
    System.out.println("kafkaStreams.state() = " + kafkaStreams.state());
    Producer<String, String> producer = configureProducer();
    Consumer<String, String> consumer = configureConsumer(OUTPUT_TOPIC);

    producer.send(new ProducerRecord<>(INPUT_TOPIC, "my-test-key", "my-test-value"));

    ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, OUTPUT_TOPIC);
    assertThat(singleRecord).isNotNull();
    assertThat(singleRecord.key()).isEqualTo("my-test-key");
    assertThat(singleRecord.value()).isEqualTo("my-test-value");

    consumer.close();
    producer.close();
  }

  private Consumer<String, String> configureConsumer(String subscriptionTopic) {
    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafkaBroker);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps)
        .createConsumer();
    consumer.subscribe(Collections.singleton(subscriptionTopic));
    return consumer;
  }

  private Producer<String, String> configureProducer() {
    Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
    producerProps.put("key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer");
    return new DefaultKafkaProducerFactory<String, String>(producerProps).createProducer();
  }

}
