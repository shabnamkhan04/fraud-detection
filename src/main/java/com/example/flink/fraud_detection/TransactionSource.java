package com.example.flink.fraud_detection;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransactionSource {

    public static DataStream<Transaction> getStream(StreamExecutionEnvironment env) {

        return env.fromElements(

                new Transaction(
                        "txn-001",
                        "user1",
                        5000,
                        "USD",
                        "merchant1",
                        System.currentTimeMillis(),
                        "US"
                ),

                new Transaction(
                        "txn-002",
                        "user1",
                        7000,
                        "USD",
                        "merchant2",
                        System.currentTimeMillis(),
                        "US"
                ),

                new Transaction(
                        "txn-003",
                        "user1",
                        300,
                        "USD",
                        "merchant3",
                        System.currentTimeMillis(),
                        "VN"
                )
        );
    }
}