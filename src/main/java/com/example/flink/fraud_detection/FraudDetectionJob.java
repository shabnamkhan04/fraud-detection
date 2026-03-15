package com.example.flink.fraud_detection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions =
                TransactionSource.getStream(env);

        DataStream<Transaction> timedStream =
                transactions.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(
                                        (event, timestamp) -> event.getTimestamp()
                                )
                );

        // PART 2 – Sliding window fraud detection
        timedStream
                .keyBy(Transaction::getUserId)
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new AmountAggregator(), new WindowResultFunction())
                .print();

        // PART 3 – Impossible travel detection
        timedStream
                .keyBy(Transaction::getUserId)
                .process(new ImpossibleTravelDetector())
                .print();

        env.execute("Real-Time Fraud Detection");
    }
}