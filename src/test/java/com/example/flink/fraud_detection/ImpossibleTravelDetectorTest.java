package com.example.flink.fraud_detection;

import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ImpossibleTravelDetectorTest {

    @Test
    public void testImpossibleTravelDetection() throws Exception {

        // Create detector
        ImpossibleTravelDetector detector = new ImpossibleTravelDetector();

        // Create Test Harness
        KeyedOneInputStreamOperatorTestHarness<String, Transaction, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(detector),
                        Transaction::getUserId,
                        Types.STRING
                );

        testHarness.open();

        // First transaction (USA)
        Transaction t1 = new Transaction(
                "txn1","user1",500,"USD","m1",1710000000000L,"US"
        );

        // Second transaction (Vietnam shortly after)
        Transaction t2 = new Transaction(
                "txn2","user1",700,"USD","m2",1710000300000L,"VN"
        );

        // Send events to Flink operator
        testHarness.processElement(t1, t1.getTimestamp());
        testHarness.processElement(t2, t2.getTimestamp());

        // Get output
        String output = testHarness.getOutput().toString();

        // Validate alert
        assertTrue(output.contains("Impossible Travel"));

        testHarness.close();
    }
    @Test
    public void testImpossibleTravelAlertGenerated() throws Exception {

        ImpossibleTravelDetector detector = new ImpossibleTravelDetector();

        KeyedOneInputStreamOperatorTestHarness<String, Transaction, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(detector),
                        Transaction::getUserId,
                        Types.STRING
                );

        testHarness.open();

        // First transaction in USA
        Transaction t1 = new Transaction(
                "txn1","user1",500,"USD","m1",1710000000000L,"US"
        );

        // Second transaction in Vietnam within 5 minutes
        Transaction t2 = new Transaction(
                "txn2","user1",700,"USD","m2",1710000300000L,"VN"
        );

        testHarness.processElement(new StreamRecord<>(t1, t1.getTimestamp()));
        testHarness.processElement(new StreamRecord<>(t2, t2.getTimestamp()));

        String output = testHarness.getOutput().toString();

        System.out.println("Output: " + output);

        // Success condition: alert should exist
        assertFalse(output.isEmpty());
        

        testHarness.close();
    }
    @Test
    public void testFirstTransactionNoAlert() throws Exception {

        ImpossibleTravelDetector detector = new ImpossibleTravelDetector();

        KeyedOneInputStreamOperatorTestHarness<String, Transaction, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(detector),
                        Transaction::getUserId,
                        Types.STRING
                );

        testHarness.open();

        Transaction t1 = new Transaction(
                "txn1","user1",300,"USD","m1",1710000000000L,"US"
        );

        testHarness.processElement(t1, t1.getTimestamp());

        String output = testHarness.getOutput().toString();

        assertTrue(output.isEmpty());

        testHarness.close();
    }
    @Test
    public void testRapidSameCountryTransactions() throws Exception {

        ImpossibleTravelDetector detector = new ImpossibleTravelDetector();

        KeyedOneInputStreamOperatorTestHarness<String, Transaction, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(detector),
                        Transaction::getUserId,
                        Types.STRING
                );

        testHarness.open();

        Transaction t1 = new Transaction(
                "txn1","user1",100,"USD","m1",1710000000000L,"US"
        );

        Transaction t2 = new Transaction(
                "txn2","user1",200,"USD","m2",1710000100000L,"US"
        );

        testHarness.processElement(t1, t1.getTimestamp());
        testHarness.processElement(t2, t2.getTimestamp());

        assertTrue(testHarness.getOutput().isEmpty());

        testHarness.close();
    }
    @Test
    public void testHighAmountTransaction() throws Exception {

        ImpossibleTravelDetector detector = new ImpossibleTravelDetector();

        KeyedOneInputStreamOperatorTestHarness<String, Transaction, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(detector),
                        Transaction::getUserId,
                        Types.STRING
                );

        testHarness.open();

        Transaction t1 = new Transaction(
                "txn1","user1",10000,"USD","m1",1710000000000L,"US"
        );

        testHarness.processElement(t1, t1.getTimestamp());

        testHarness.close();
    }

   
}