package com.example.flink.fraud_detection;

import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class EventTimeImpossibleTravelTest {

    @Test
    public void testEventTimeProcessing() throws Exception {

        ImpossibleTravelDetector detector = new ImpossibleTravelDetector();

        KeyedOneInputStreamOperatorTestHarness<String, Transaction, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(detector),
                        Transaction::getUserId,
                        Types.STRING
                );

        testHarness.open();

        Transaction t1 = new Transaction(
                "txn1","user1",400,"USD","m1",1710000000000L,"US"
        );

        Transaction t2 = new Transaction(
                "txn2","user1",500,"USD","m2",1710000300000L,"VN"
        );

        // send first event
        testHarness.processElement(new StreamRecord<>(t1, t1.getTimestamp()));

        // advance watermark
        testHarness.processWatermark(1710000100000L);

        // send second event
        testHarness.processElement(new StreamRecord<>(t2, t2.getTimestamp()));

        String output = testHarness.getOutput().toString();

        assertTrue(output.contains("Impossible"));

        testHarness.close();
    }
    @Test
    public void testOutOfOrderEvents() throws Exception {

        ImpossibleTravelDetector detector = new ImpossibleTravelDetector();

        KeyedOneInputStreamOperatorTestHarness<String, Transaction, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(detector),
                        Transaction::getUserId,
                        Types.STRING
                );

        testHarness.open();

        Transaction t1 = new Transaction(
                "txn1","user1",300,"USD","m1",1710000300000L,"VN"
        );

        Transaction t2 = new Transaction(
                "txn2","user1",400,"USD","m2",1710000000000L,"US"
        );

        testHarness.processElement(t1, t1.getTimestamp());
        testHarness.processElement(t2, t2.getTimestamp());

        testHarness.close();
    }
    
    @Test
    public void testLateEventHandling() throws Exception {

        ImpossibleTravelDetector detector = new ImpossibleTravelDetector();

        KeyedOneInputStreamOperatorTestHarness<String, Transaction, String> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(detector),
                        Transaction::getUserId,
                        Types.STRING
                );

        harness.open();

        Transaction lateEvent = new Transaction(
                "txn1","user1",200,"USD","m1",1700000000000L,"US"
        );

        harness.processElement(lateEvent, lateEvent.getTimestamp());

        harness.close();
    }
    @Test
    public void testHighVolumeTransactions() throws Exception {

        ImpossibleTravelDetector detector = new ImpossibleTravelDetector();

        KeyedOneInputStreamOperatorTestHarness<String, Transaction, String> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(detector),
                        Transaction::getUserId,
                        Types.STRING
                );

        harness.open();

        for (int i = 0; i < 1000; i++) {

            Transaction t = new Transaction(
                    "txn"+i,"user1",100,"USD","m1",
                    1710000000000L + i * 1000,"US"
            );

            harness.processElement(t, t.getTimestamp());
        }

        harness.close();
    }
}