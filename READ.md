# Real-Time Fraud Detection using Apache Flink

## Overview

This project implements a **real-time fraud detection system** using **Apache Flink**.
The application processes streaming transaction data and detects suspicious activities such as:

* High transaction volume within a short time window
* Impossible travel between different countries

The system uses **event-time processing, watermarks, sliding windows, and stateful stream processing** to analyze transaction streams in real time.

---

## Features

### 1. Sliding Window Fraud Detection

Detects fraud when a user spends more than **$10,000 within 5 minutes**.

Example rule:

```
If total spending > $10,000 in 5 minutes → FRAUD ALERT
```

### 2. Impossible Travel Detection

Detects suspicious activity when a user makes transactions from **different countries within a short time**.

Example:

```
Transaction 1: USA
Transaction 2: Vietnam
→ IMPOSSIBLE TRAVEL ALERT
```

### 3. Event-Time Processing

Transactions are processed using **event-time semantics** with **watermarks** to handle out-of-order events.

---

## Architecture

Transaction Source
↓
Assign Timestamps & Watermarks
↓
Key By User ID
↓
Sliding Window Aggregation
↓
Fraud Detection
↓
Impossible Travel Detection
↓
Alert Output

---

## Technologies Used

* Java
* Apache Flink
* Maven
* Event-Time Processing
* Watermarks
* Sliding Windows
* Stateful Processing

---

## Project Structure

```
fraud-detection
│
├── src
│   └── main
│       └── java
│           └── com.example.flink.fraud_detection
│               ├── FraudDetectionJob.java
│               ├── Transaction.java
│               ├── TransactionSource.java
│               ├── AmountAggregator.java
│               ├── WindowResultFunction.java
│               └── ImpossibleTravelDetector.java
│
├── pom.xml
├── README.md
└── .gitignore
```

---

## Sample Output

```
IMPOSSIBLE TRAVEL ALERT: user1 from US to VN
FRAUD ALERT: User user1 spent $12300.0 in last 5 minutes
```

---

## How to Run

1. Clone the repository

```
git clone https://github.com/shabnamkhan04/fraud-detection.git
```

2. Navigate to project directory

```
cd fraud-detection
```

3. Build the project

```
mvn clean install
```

4. Run the application

Run the main class:

```
FraudDetectionJob.java
```

The application starts a **local Flink MiniCluster** and processes simulated transaction events.

---

## Future Improvements

* Integrate **Apache Kafka or Amazon Kinesis** as streaming source
* Deploy on **AWS Managed Service for Apache Flink**
* Store alerts in **Amazon S3 or DynamoDB**
* Add real-world transaction datasets

---

## Author

Shabnam Khan
Data Engineering Project – Real-Time Fraud Detection with Apache Flink
