# Excel to Kafka Dataflow Connector (Java / Spring Boot)

An event-driven Spring Boot service that continuously monitors a directory for Excel spreadsheets (`.xlsx`), parses row data using Apache POI, and streams each row as a structured JSON message to an Apache Kafka topic.

---

## 🏗️ Architecture & Pipeline Summary

1. **Directory Monitoring:** Uses Spring File Watcher to continuously monitor a target directory for newly dropped or updated Excel (`.xlsx`) files.
2. **Spreadsheet Parsing (Apache POI):** Reads incoming `.xlsx` files, extracts Row 0 as dynamic column headers, and maps each subsequent row's cell values into header-driven key-value pairs.
3. **Data Model Mapping:** Maps each spreadsheet row into a Java object (`RowMessage`), preserving file metadata, row index, and key-value payload data.
4. **Asynchronous JSON Streaming:** Uses Spring's `KafkaTemplate` to serialize each `RowMessage` into a JSON payload and publish it asynchronously to the `excel-file-updates` Kafka topic.
5. **Ordered Partitioning Strategy:** Constructs a composite message key formatted as `filename:rowIndex` (e.g., `test.xlsx:1`). Since Kafka hashes the key to determine partition placement, keeping a consistent filename prefix routes all rows of a file to the exact same partition, guaranteeing strict sequential processing by downstream consumers.

---

## 🛠️ Tech Stack

- **Java 17+**
- **Spring Boot 3.x** (`spring-kafka`, `spring-boot-starter-web`)
- **Apache POI** (Excel file parsing)
- **Docker & Docker Compose** (Kafka & Zookeeper local execution)

---

## 🚀 Quick Start

### 1. Start Local Infrastructure
```bash
docker-compose up -d
```