package com.example.flink.fraud_detection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/*
 * TransactionDeserializationSchema
 *
 * This class is responsible for converting raw Kafka messages
 * (in byte[] format) into Java Transaction objects.
 *
 * Kafka messages usually arrive as JSON strings. This schema
 * uses Jackson ObjectMapper to convert the JSON message
 * into a Transaction POJO.
 */

public class TransactionDeserializationSchema
        implements DeserializationSchema<Transaction> {

    /*
     * ObjectMapper from Jackson library
     * Used to convert JSON data into Java objects.
     */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /*
     * deserialize()
     *
     * This method is called by Flink every time a message
     * is read from Kafka.
     *
     * Parameter:
     * message -> raw message received from Kafka in byte[] format
     *
     * Returns:
     * Transaction object parsed from JSON
     */

    @Override
    public Transaction deserialize(byte[] message) throws IOException {

        try {

            /*
             * Convert JSON byte[] message into Transaction object
             * Example JSON:
             * {
             *   "transactionId":"txn-001",
             *   "userId":"user-123",
             *   "amount":500,
             *   "currency":"USD",
             *   "merchantId":"m1",
             *   "timestamp":1710000000000,
             *   "location":"US"
             * }
             */
            return objectMapper.readValue(message, Transaction.class);

        } catch (Exception e) {

            /*
             * If JSON is malformed or cannot be parsed,
             * we log the invalid record instead of stopping
             * the Flink job.
             */
            System.out.println("Invalid record skipped: " + new String(message));

            /*
             * Returning null allows us to skip bad records
             * without crashing the streaming pipeline.
             */
            return null;
        }
    }

    /*
     * isEndOfStream()
     *
     * Used by Flink to indicate if the stream has ended.
     * For Kafka streams, this is always false because
     * the stream is continuous.
     */
    @Override
    public boolean isEndOfStream(Transaction nextElement) {
        return false;
    }

    /*
     * getProducedType()
     *
     * Tells Flink the output type produced by this
     * deserialization schema.
     *
     * In this case, it produces Transaction objects.
     */
    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}