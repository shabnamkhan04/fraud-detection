package com.example.flink.fraud_detection;

public class Transaction {

    private String transactionId;
    private String userId;
    private double amount;
    private String currency;
    private String merchantId;
    private long timestamp;
    private String country;

    // Empty constructor (required for Flink)
    public Transaction() {}

    public Transaction(String transactionId, String userId, double amount,
                       String currency, String merchantId, long timestamp, String country) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.currency = currency;
        this.merchantId = merchantId;
        this.timestamp = timestamp;
        this.country = country;
    }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }
}