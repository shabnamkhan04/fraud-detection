package com.example.flink.fraud_detection;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.time.Time;

public class ImpossibleTravelDetector
        extends KeyedProcessFunction<String, Transaction, String> {

    private ValueState<Transaction> lastTransaction;

    @Override
    public void open(Configuration parameters) {

        // TTL configuration
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.minutes(30))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();

        // State descriptor
        ValueStateDescriptor<Transaction> descriptor =
                new ValueStateDescriptor<>("lastTransaction", Transaction.class);

        descriptor.enableTimeToLive(ttlConfig);

        lastTransaction = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            Transaction current,
            Context ctx,
            Collector<String> out) throws Exception {

        Transaction previous = lastTransaction.value();

        if (previous != null) {

            long timeDiff =
                    current.getTimestamp() - previous.getTimestamp();

            if (!current.getCountry().equals(previous.getCountry())
                    && timeDiff < 10 * 60 * 1000) {

                out.collect("IMPOSSIBLE TRAVEL ALERT: "
                        + current.getUserId()
                        + " from "
                        + previous.getCountry()
                        + " to "
                        + current.getCountry());
            }
        }

        lastTransaction.update(current);
    }
}