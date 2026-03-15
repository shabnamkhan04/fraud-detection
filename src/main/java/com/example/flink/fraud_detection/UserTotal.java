package com.example.flink.fraud_detection;

import java.io.Serializable;

public class UserTotal implements Serializable {

    private String userId;
    private double amount;

    public UserTotal(){}

    public UserTotal(String userId,double amount){
        this.userId=userId;
        this.amount=amount;
    }

    public String getUserId(){
        return userId;
    }

    public double getAmount(){
        return amount;
    }

    @Override
    public String toString(){
        return "UserTotal{userId='"+userId+"', amount="+amount+"}";
    }
}
