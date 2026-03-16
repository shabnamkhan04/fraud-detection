package com.example.flink.fraud_detection;

import org.apache.flink.api.common.functions.AggregateFunction;

/*
 * AmountAggregator
 *
 * This class performs aggregation of transaction amounts in a Flink window.
 *
 * It implements Flink's AggregateFunction interface which is used for
 * efficient incremental aggregation.
 *
 * Type Parameters:
 * INPUT  -> Transaction (incoming stream element)
 * ACC    -> Double (accumulator type used during aggregation)
 * OUTPUT -> Double (final result of aggregation)
 */

public class AmountAggregator
        implements AggregateFunction<Transaction, Double, Double> {

    /*
     * createAccumulator()
     *
     * This method initializes the accumulator.
     * The accumulator holds the intermediate aggregation result.
     *
     * Here we start with 0.0 because we are summing transaction amounts.
     */
    @Override
    public Double createAccumulator() {
        return 0.0;
    }

    /*
     * add()
     *
     * This method is called for every incoming transaction.
     *
     * Parameters:
     * value        -> incoming Transaction event
     * accumulator  -> current running total
     *
     * Returns:
     * Updated accumulator with the new transaction amount added.
     */
    @Override
    public Double add(Transaction value, Double accumulator) {
        return accumulator + value.getAmount();
    }

    /*
     * getResult()
     *
     * This method returns the final aggregated value
     * when the window computation finishes.
     *
     * In this case, it returns the total transaction amount
     * accumulated in the window.
     */
    @Override
    public Double getResult(Double accumulator) {
        return accumulator;
    }

    /*
     * merge()
     *
     * This method is used when Flink merges two accumulators.
     * This typically happens in session windows or when parallel
     * computations need to combine results.
     *
     * It simply adds the two partial totals together.
     */
    @Override
    public Double merge(Double a, Double b) {
        return a + b;
    }
}