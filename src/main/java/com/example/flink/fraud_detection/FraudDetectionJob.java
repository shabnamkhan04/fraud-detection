package com.example.flink.fraud_detection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/*
 * FraudDetectionJob
 *
 * This is the main class that runs the Apache Flink streaming job.
 * It connects all parts of the fraud detection pipeline:
 *
 * Kafka Source → Transaction Parsing → Event Time Handling
 * → Window Aggregation → Fraud Detection → Output Alerts
 */

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {

        /*
         * Create the Flink streaming execution environment.
         * This is the entry point for all Flink streaming applications.
         */
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        /*
         * Configure how often Flink should generate watermarks.
         * Watermarks help handle event-time processing.
         */
        env.getConfig().setAutoWatermarkInterval(1000);

        /*
         * Step 1: Read transaction data from Kafka.
         *
         * TransactionSource returns a DataStream<String>
         * where each element is a JSON message.
         */
        DataStream<Transaction> transactions =
                TransactionSource
                        .getStream(env)
                        .map(json -> Transaction.fromJson(json));

        /*
         * Step 2: Assign event timestamps and watermarks.
         *
         * This allows Flink to process events based on the
         * time when the transaction actually happened,
         * not when it arrived in the system.
         *
         * We allow up to 5 seconds of out-of-order events.
         */
        DataStream<Transaction> transactionStream =
                transactions.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        (event, timestamp) -> event.getTimestamp()
                                )
                );

        /*
         * Step 3: Print incoming events for debugging.
         * This helps verify that data is correctly read from Kafka.
         */
        transactionStream.print("EVENT");

        /*
         * Step 4: Sliding Window Aggregation
         *
         * Group transactions by userId.
         * Create a sliding window:
         * - Window size: 30 seconds
         * - Slide interval: 10 seconds
         *
         * This calculates the total transaction amount
         * per user within the window.
         */
        transactionStream
                .keyBy(Transaction::getUserId)
                .window(org.apache.flink.streaming.api.windowing.assigners
                        .SlidingProcessingTimeWindows
                        .of(Time.seconds(30), Time.seconds(10)))
                .aggregate(new AmountAggregator(), new WindowResultFunction())
                .print("WINDOW RESULT");

        /*
         * Step 5: Impossible Travel Detection
         *
         * Detect suspicious activity where a user
         * performs transactions from different countries
         * within a short time period.
         *
         * This uses a KeyedProcessFunction with state
         * to remember the last country and timestamp.
         */
        transactionStream
                .keyBy(Transaction::getUserId)
                .process(new ImpossibleTravelDetector())
                .print("TRAVEL ALERT");

        /*
         * Step 6: Start the Flink streaming job.
         */
        env.execute("Real-Time Fraud Detection");
    }
}