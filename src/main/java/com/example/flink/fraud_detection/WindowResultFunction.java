package com.example.flink.fraud_detection;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
 * WindowResultFunction
 *
 * This class processes the result of a windowed aggregation in Apache Flink.
 * It receives the aggregated total transaction amount for a user within
 * a specific time window (for example, 5 minutes).
 *
 * If the total spending of the user exceeds a predefined threshold,
 * it emits a fraud alert message.
 */

public class WindowResultFunction
        extends ProcessWindowFunction<Double, String, String, TimeWindow> {

    /*
     * process() method is called once for each window after aggregation is complete.
     *
     * Parameters:
     * userId   -> The key of the stream (in this case, the user ID).
     * context  -> Provides metadata about the window (start time, end time, etc).
     * totals   -> Iterable containing the aggregated result from the previous step.
     * out      -> Collector used to emit results downstream.
     */

    @Override
    public void process(String userId,
                        Context context,
                        Iterable<Double> totals,
                        Collector<String> out) {

        /*
         * Since we used an AggregateFunction before this step,
         * the iterable contains only one value:
         * the total transaction amount in the window.
         */
        Double total = totals.iterator().next();

        /*
         * Fraud detection rule:
         * If the total amount spent by a user in the last 5 minutes
         * exceeds $10,000, we generate a fraud alert.
         */
        if (total > 10000) {

            /*
             * Emit an alert message indicating suspicious activity.
             * This message can later be sent to Kafka, logs, or monitoring systems.
             */
            out.collect(
                    "TOTAL_EXCEEDED: User " + userId +
                    " spent $" + total +
                    " in last 5 minutes"
            );
        }
    }
}