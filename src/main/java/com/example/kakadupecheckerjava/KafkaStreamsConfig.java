package com.example.kakadupecheckerjava;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
class KafkaStreamsConfig {

    @Value("${spring.application.name.applicationID}")
    public String applicationID;

    @Value("${spring.application.name.windowRetentionDuration}")
    public String windowRetentionDuration;

    @Value("${spring.application.name.transactionLogName}")
    public String transactionLogName;

    @Value("${spring.application.name.inputTopic}")
    public String inputTopic;

    @Value("${spring.application.name.outputTopic}")
    public String outputTopic;

    @Value("${spring.application.name.outputDuplicateTopic}")
    public String outputDuplicateTopic;

    @Value("${spring.application.name.bootstrapServers}")
    public String bootstrapServers;

    @Value("${spring.application.name.statedirpath:\"\"}")
    public String statedirpath;

    @Value("${max.request.name.size:2097176}")
    public String maxRequestSize;

    @Bean
    public KafkaStreams kafkaStreams() {
        EventDeduplication eventDeduplication = new EventDeduplication(
            applicationID, Duration.ofHours(Long.parseLong(windowRetentionDuration)),
            transactionLogName, inputTopic, outputTopic,
            outputDuplicateTopic, bootstrapServers
        );
        Properties streamsConfiguration = eventDeduplication.getStreamsConfiguration(bootstrapServers, statedirpath);
        Topology builder = eventDeduplication.createTopology();
        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
        //Add the shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        return streams;
    }
}

