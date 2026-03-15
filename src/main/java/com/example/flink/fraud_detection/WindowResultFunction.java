package com.example.flink.fraud_detection;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultFunction
        extends ProcessWindowFunction<Double, String, String, TimeWindow> {

    @Override
    public void process(String key,
                        Context context,
                        Iterable<Double> input,
                        Collector<String> out) {

        double total = input.iterator().next();

        if (total > 10000) {
            out.collect("FRAUD ALERT: User " + key +
                    " spent $" + total +
                    " in last 5 minutes");
        }
    }
}
