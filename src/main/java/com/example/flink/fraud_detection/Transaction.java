package com.example.flink.fraud_detection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/*
 * Transaction Class
 *
 * This class represents a single transaction event in the fraud detection system.
 * It is used as a POJO (Plain Old Java Object) in Apache Flink streams.
 *
 * Each Kafka message containing transaction data will be converted
 * into an object of this class.
 *
 * @JsonIgnoreProperties(ignoreUnknown = true)
 * This annotation tells Jackson to ignore any extra JSON fields
 * that are not defined in this class.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction {

    /*
     * ObjectMapper is used to convert JSON strings
     * into Transaction objects.
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    /* Unique transaction identifier */
    private String transactionId;

    /* ID of the user who made the transaction */
    private String userId;

    /* Transaction amount */
    private double amount;

    /* Country where the transaction occurred */
    private String country;

    /* Event timestamp (epoch milliseconds) */
    private long timestamp;

    /* Currency used in the transaction (USD, EUR, etc.) */
    private String currency;

    /* Merchant identifier where the transaction happened */
    private String merchantId;

    /*
     * Default constructor
     *
     * Required for Flink POJO serialization and
     * JSON deserialization frameworks.
     */
    public Transaction() {}

    /*
     * Parameterized constructor
     *
     * This constructor is useful for:
     * - Unit testing
     * - Creating transaction objects manually
     */
    public Transaction(String transactionId,
                       String userId,
                       double amount,
                       String currency,
                       String merchantId,
                       long timestamp,
                       String country) {

        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.currency = currency;
        this.merchantId = merchantId;
        this.timestamp = timestamp;
        this.country = country;
    }

    /* Getter methods used by Flink and other classes */

    public String getTransactionId() { return transactionId; }

    public String getUserId() { return userId; }

    public double getAmount() { return amount; }

    public String getCountry() { return country; }

    public long getTimestamp() { return timestamp; }

    public String getCurrency() { return currency; }

    public String getMerchantId() { return merchantId; }

    /*
     * Helper method to convert JSON string
     * into a Transaction object.
     *
     * Useful for:
     * - Local testing
     * - Debugging
     * - Manual parsing of Kafka messages
     */
    public static Transaction fromJson(String json) {
        try {
            return mapper.readValue(json, Transaction.class);
        } catch (Exception e) {
            throw new RuntimeException("Invalid JSON", e);
        }
    }

    /*
     * toString() method
     *
     * Used for logging, debugging, and printing
     * transaction details in a readable format.
     */
    @Override
    public String toString() {
        return "Transaction{" +
                "userId='" + userId + '\'' +
                ", amount=" + amount +
                ", country='" + country + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}