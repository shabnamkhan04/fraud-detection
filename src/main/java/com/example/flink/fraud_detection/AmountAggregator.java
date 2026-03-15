package com.example.flink.fraud_detection;

import org.apache.flink.api.common.functions.AggregateFunction;

public class AmountAggregator
        implements AggregateFunction<Transaction, Double, Double> {

    @Override
    public Double createAccumulator() {
        return 0.0;
    }

    @Override
    public Double add(Transaction value, Double accumulator) {
        return accumulator + value.getAmount();
    }

    @Override
    public Double getResult(Double accumulator) {
        return accumulator;
    }

    @Override
    public Double merge(Double a, Double b) {
        return a + b;
    }
}