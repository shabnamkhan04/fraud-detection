package com.example.flink.fraud_detection;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/*
 * ImpossibleTravelDetector
 *
 * This class detects suspicious transactions where a user appears
 * to make transactions from different countries within a very short time.
 *
 * Example:
 * User transaction in US → 2 minutes later → transaction in Vietnam
 * This is physically impossible travel, so we trigger an alert.
 */

public class ImpossibleTravelDetector
        extends KeyedProcessFunction<String, Transaction, String> {

    /*
     * State to store the last country where the user made a transaction
     */
    private ValueState<String> lastCountryState;

    /*
     * State to store the timestamp of the last transaction
     */
    private ValueState<Long> lastTimestampState;

    /*
     * open() method is called once when the operator starts.
     * Here we initialize Flink state variables.
     */
    @Override
    public void open(Configuration parameters) {

        /*
         * State TTL configuration
         * State will expire automatically after 30 minutes
         * to prevent memory growth.
         */
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.minutes(30))
                .build();

        /*
         * State descriptor for last country
         */
        ValueStateDescriptor<String> countryDescriptor =
                new ValueStateDescriptor<>("lastCountry", String.class);

        countryDescriptor.enableTimeToLive(ttlConfig);

        /*
         * Register the state with Flink runtime
         */
        lastCountryState = getRuntimeContext().getState(countryDescriptor);

        /*
         * State descriptor for last transaction timestamp
         */
        ValueStateDescriptor<Long> timeDescriptor =
                new ValueStateDescriptor<>("lastTimestamp", Long.class);

        timeDescriptor.enableTimeToLive(ttlConfig);

        lastTimestampState = getRuntimeContext().getState(timeDescriptor);
    }

    /*
     * processElement()
     *
     * This method is called for every transaction event.
     */
    @Override
    public void processElement(
            Transaction transaction,
            Context ctx,
            Collector<String> out) throws Exception {

        /*
         * Retrieve previous state values
         */
        String lastCountry = lastCountryState.value();
        Long lastTimestamp = lastTimestampState.value();

        /*
         * Check if previous transaction exists
         */
        if (lastCountry != null && lastTimestamp != null) {

            /*
             * Calculate time difference between transactions
             */
            long timeDiff = transaction.getTimestamp() - lastTimestamp;

            /*
             * Fraud rule:
             * If user changes country within 10 minutes,
             * it is considered impossible travel.
             */
            if (!lastCountry.equals(transaction.getCountry())
                    && timeDiff < 600000) {   // 600000 ms = 10 minutes

                /*
                 * Emit fraud alert
                 */
                out.collect(
                        "IMPOSSIBLE TRAVEL ALERT: " +
                        transaction.getUserId() +
                        " from " + lastCountry +
                        " to " + transaction.getCountry()
                );
            }
        }

        /*
         * Update state with current transaction data
         */
        lastCountryState.update(transaction.getCountry());
        lastTimestampState.update(transaction.getTimestamp());
    }
}