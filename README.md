# Dataflow Connectors - File Connector Service

A lightweight, high-performance Spring Boot service that continuously monitors directory paths for file events (uploads and modifications) on Excel files (`.xlsx`). It performs stateful, row-level delta detection using row hashing and streams real-time data modifications (`ADDED`, `UPDATED`) directly to an Apache Kafka topic.

---

## 🌟 Key Features

* **Stateful Row-Level Delta Detection:** Uses an in-memory `ConcurrentHashMap` state store to track row content hashes (`filename:rowIndex` $\rightarrow$ `hash`).
* **Granular Change Classifications:** 
  * `ADDED`: Newly inserted rows.
  * `UPDATED`: Existing rows where cell values have changed.
  * `UNCHANGED`: Unmodified rows (filtered out automatically to prevent redundant event streaming).
* **Automated Directory Polling:** Built-in `DirectoryWatcher` component utilizing Spring `@Scheduled` tasks to monitor file modification timestamps (`lastModified`) in real-time.
* **Robust Type Safety:** Uses Apache POI `DataFormatter` to handle mixed cell types (numeric, formula, string, dates) cleanly without runtime exceptions.
* **Kafka Event Publishing:** Converts non-`UNCHANGED` rows into structured `RowMessage` JSON payloads and publishes them to the `excel-file-updates` Kafka topic.

---

## 🏗️ Architecture & Component Overview

```
 [ Input Directory: data/input ]
               │
               ▼
     [ DirectoryWatcher ] ──(Monitors file lastModified timestamps)
               │
               ▼
     [ ExcelFileWatcher ] ──(Parses rows with Apache POI DataFormatter)
               │
               ├─── (Calculates Row Hash & Compares with State Store)
               │
               ▼
      [ KafkaRowPublisher ] ──(Streams RowMessage JSON events)
               │
               ▼
    [ Kafka Topic: excel-file-updates ]
```

### Core Classes

1. **`DirectoryWatcher.java`**
   * Periodically scans the configured input folder (`data/input`) every 3 seconds (`fixedDelay = 3000`).
   * Detects new files or modified timestamps and invokes the file processor.
2. **`ExcelFileWatcher.java`**
   * Reads header row and data rows.
   * Generates hash values for each row and checks against `fileStateStore`.
   * Outputs precise console summary logs:
     `📊 [test.xlsx] Delta Summary: 0 Added, 1 Updated, 10 Unchanged`
3. **`KafkaRowPublisher.java` / `KafkaProducerConfig.java`**
   * Configures the Kafka producer and handles event transmission.
4. **`RowMessage.java`**
   * Encapsulates event metadata (`fileName`, `sheetName`, `rowIndex`, `data`, `changeType`, `timestamp`).

---

## 🚀 Getting Started

### Prerequisites

* **Java 17** or higher
* **Apache Maven 3.8+**
* **Docker & Docker Compose** (for Kafka & Zookeeper)

### 1. Start Infrastructure (Kafka & Zookeeper)

Ensure your Kafka broker is up and running on `localhost:9092`:

```bash
docker-compose up -d
```

### 2. Build the Application

Compile and package the Spring Boot application using Maven:

```bash
mvn clean compile
```

### 3. Run the Application

Start the Spring Boot file connector service:

```bash
mvn spring-boot:run
```

---

## 🧪 Real-Time Verification & Testing

### Step 1: Monitor Live Kafka Stream

Open a terminal to consume real-time row events published to Kafka:

```bash
docker exec -it kafka /usr/bin/kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic excel-file-updates \
  --property print.key=true \
  --property key.separator=" : "
```

### Step 2: Test File Upload (`ADDED`)

Drop an Excel file (`test.xlsx`) into `data/input/`.

* **Application Terminal Output:**
  ```text
  📊 [test.xlsx] Delta Summary: 11 Added, 0 Updated, 0 Unchanged
  ```
* **Kafka Consumer Output:** 11 JSON event payloads tagged with `"changeType": "ADDED"`.

### Step 3: Test File Update (`UPDATED`)

Open `data/input/test.xlsx` in Excel, edit any single cell value, and press **Ctrl + S**.

* **Application Terminal Output:**
  ```text
  📊 [test.xlsx] Delta Summary: 0 Added, 1 Updated, 10 Unchanged
  ```
* **Kafka Consumer Output:** Exactly 1 JSON event payload containing `"changeType": "UPDATED"`.

---

## 📄 Example JSON Message Payload

```json
{
  "fileName": "test.xlsx",
  "sheetName": "Sheet1",
  "rowIndex": 1,
  "data": {
    "First Name": "Ankit",
    "Last Name": "Kaushik",
    "Gender": "Male",
    "Country": "India",
    "Age": "30",
    "Date": "04/08/2026",
    "Id": "1001"
  },
  "changeType": "UPDATED",
  "timestamp": 1785843877850
}
```

---

## 🛠️ Project Structure

```text
file-connector/
├── data/
│   └── input/                  # Monitored input folder for .xlsx files
├── src/
│   └── main/
│       ├── java/com/dataflow/connector/file/
│       │   ├── model/
│       │   │   └── RowMessage.java
│       │   ├── publisher/
│       │   │   └── KafkaRowPublisher.java
│       │   ├── reader/
│       │   │   └── ExcelFileReader.java
│       │   ├── watcher/
│       │   │   ├── DirectoryWatcher.java
│       │   │   └── ExcelFileWatcher.java
│       │   └── FileConnectorApplication.java
│       └── resources/
│           └── application.yml
├── pom.xml
└── README.md
```
