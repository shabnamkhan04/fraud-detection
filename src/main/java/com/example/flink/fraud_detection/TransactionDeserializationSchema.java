package com.example.flink.fraud_detection;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class TransactionDeserializationSchema implements DeserializationSchema<Transaction> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        try {
            return mapper.readValue(message, Transaction.class);
        } catch (Exception e) {
            // Skip malformed record
            System.out.println("Invalid record skipped");
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Transaction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}