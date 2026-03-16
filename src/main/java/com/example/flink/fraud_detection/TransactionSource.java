package com.example.flink.fraud_detection;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

/*
 * TransactionSource
 *
 * This class is responsible for reading transaction data from
 * an Apache Kafka topic and providing it as a Flink DataStream.
 *
 * Kafka acts as the streaming data source where transaction
 * events are continuously produced.
 */

public class TransactionSource {

    /*
     * getStream() method
     *
     * This method creates a Kafka source and connects it to
     * the Flink execution environment.
     *
     * Parameter:
     * env -> Flink StreamExecutionEnvironment used to run the job.
     *
     * Returns:
     * DataStream<String> containing raw transaction events
     * received from Kafka.
     */

    public static DataStream<String> getStream(StreamExecutionEnvironment env) {

        /*
         * Create a KafkaSource using the builder pattern.
         * This defines how Flink will connect to Kafka.
         */
        KafkaSource<String> source = KafkaSource.<String>builder()

                /* Address of Kafka broker */
                .setBootstrapServers("localhost:9092")

                /* Kafka topic from which transactions are read */
                .setTopics("transactions")

                /* Consumer group ID used by Kafka */
                .setGroupId("fraud-detection-group")

                /*
                 * Start reading from the earliest available
                 * offset in the topic.
                 */
                .setStartingOffsets(OffsetsInitializer.earliest())

                /*
                 * Deserializer to convert Kafka message
                 * bytes into String format.
                 */
                .setValueOnlyDeserializer(new SimpleStringSchema())

                /* Build the Kafka source */
                .build();

        /*
         * Add the Kafka source to the Flink environment
         * and create a DataStream from it.
         *
         * WatermarkStrategy.noWatermarks() means we are not
         * using event-time processing here.
         */
        return env.fromSource(
                source,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );
    }
}