package com.example.flink.fraud_detection;

import java.io.Serializable;

/*
 * UserTotal Class
 *
 * This is a simple Java POJO used to store the total transaction
 * amount for a specific user.
 *
 * It implements Serializable so that Apache Flink can transfer
 * this object across distributed nodes in the cluster.
 */

public class UserTotal implements Serializable {

    /* Unique identifier of the user */
    private String userId;

    /* Total transaction amount associated with the user */
    private double amount;

    /*
     * Default constructor
     *
     * Required by Flink and serialization frameworks
     * such as Jackson or Kryo.
     */
    public UserTotal(){}

    /*
     * Parameterized constructor
     *
     * Used to create a UserTotal object with userId and amount.
     */
    public UserTotal(String userId,double amount){
        this.userId=userId;
        this.amount=amount;
    }

    /*
     * Getter method for userId
     */
    public String getUserId(){
        return userId;
    }

    /*
     * Getter method for amount
     */
    public double getAmount(){
        return amount;
    }

    /*
     * toString() method
     *
     * Useful for logging, debugging, and printing
     * the object in readable format.
     */
    @Override
    public String toString(){
        return "UserTotal{userId='"+userId+"', amount="+amount+"}";
    }
}