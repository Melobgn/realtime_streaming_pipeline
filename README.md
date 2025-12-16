# Real-Time Streaming Pipeline with Spark, Kafka, and Delta Lake

## Overview

This project implements a real-time data streaming pipeline using Apache Spark Structured Streaming, Apache Kafka, and Delta Lake. It demonstrates core Data Engineering concepts such as streaming ingestion, data cleaning, fault tolerance, checkpointing, and layered data architecture (Medallion Architecture).

The pipeline is divided into two main parts:

- **Part 2.1**: File-based streaming ingestion (JSON files → Delta Lake Bronze)
- **Part 2.2**: Kafka-based real-time streaming (Kafka → Delta Lake Silver)

## Architecture Overview

### Part 2.1 – File-Based Streaming (Bronze Layer)

- **Input**: JSON files (IoT data)
- **Processing**: Spark Structured Streaming
- **Output**: Delta Lake (Bronze)

### Part 2.2 – Kafka-Based Streaming (Silver Layer)

- **Input**: Kafka Producer (sensor simulator) → Kafka topic (sensor_data)
- **Processing**: Spark Structured Streaming
- **Output**: Delta Lake (Silver)

Kafka is deployed using Docker (Confluent + ZooKeeper), while Spark and Python scripts run locally.

## Technologies Used

- Apache Spark Structured Streaming
- Apache Kafka
- ZooKeeper
- Delta Lake
- Docker & Docker Compose
- Python 3
- kafka-python

## Project Structure

```
.
├── docker-compose.yml
├── kafka_producer.py
├── spark_kafka_consumer.py
├── read_delta.py
├── read_bronze.py
├── read_silver.py
├── streaming_files_to_delta_bronze.py
├── lake/
│   ├── bronze/
│   │   └── sensor_data/
│   └── silver/
│       └── sensor_data/
├── checkpoints/
│   ├── bronze_sensor/
│   │   ├── metadata
│   │   ├── commits/
│   │   ├── offsets/
│   │   └── sources/
│   └── silver_sensor_data/
│       ├── metadata
│       ├── commits/
│       ├── offsets/
│       └── sources/
└── README.md
```

> **Note**: Generated data (Parquet files, Delta logs, checkpoints) is intentionally excluded from version control.

## Part 2.1 – File-Based Streaming (Bronze)

### Description

This pipeline ingests IoT sensor data from JSON files using Spark Structured Streaming. It applies basic data cleaning and validation before writing the data to a Delta Lake Bronze table.

### Key Features

- Explicit schema enforcement
- Timestamp parsing
- Null filtering and basic sanity checks
- Append-only Delta Lake writes
- Dedicated checkpoint directory for fault tolerance

### Script

- `streaming_files_to_delta_bronze.py`

The pipeline runs continuously and processes newly added JSON files only.

### Reading the Delta Bronze Table

#### Description

A script to read and display the most recent records from the Delta Bronze table.

#### Script

- `read_bronze.py`

#### Run

```bash
python3 read_bronze.py
```

## Part 2.2 – Kafka-Based Streaming (Silver)

### Kafka Setup (Docker)

Kafka and ZooKeeper are deployed using Docker:

```bash
docker compose up -d
```

### Create the Kafka Topic

In a separate terminal:

```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic sensor_data \
  --partitions 3 \
  --replication-factor 1
```

### Kafka Producer – Sensor Simulator

#### Description

The producer simulates IoT sensors and sends one JSON message per second to the Kafka topic.

#### Script

- `kafka_producer.py`

#### Run

```bash
python3 kafka_producer.py
```

### Spark Kafka Consumer – Delta Silver

#### Description

This Spark Structured Streaming job consumes messages from Kafka, cleans and validates the data, and writes the results to a Delta Lake Silver table.

#### Key Features

- Kafka source with offset management
- JSON parsing with explicit schema
- Data cleaning and validation
- Delta Lake output (append mode)
- Spark checkpointing for fault tolerance

#### Script

- `spark_kafka_consumer.py`

#### Run

```bash
python3 spark_kafka_consumer.py
```

### Reading the Delta Silver Table

#### Description

A script to read and display the most recent records from the Delta Silver table.

#### Script

- `read_silver.py`

#### Run

```bash
python3 read_silver.py
```

> **Note**: Output directories (`lake/silver/sensor_data/` and `checkpoints/silver_sensor_data/`) are created automatically.

## Execution Order Summary

1. Start Kafka and ZooKeeper:

   ```bash
   docker compose up -d
   ```

2. Create the Kafka topic:

   ```bash
   kafka-topics --create ...
   ```

3. Start the Kafka producer:

   ```bash
   python3 kafka_producer.py
   ```

4. Start the Spark Kafka consumer:

   ```bash
   python3 spark_kafka_consumer.py
   ```

5. Read the Delta Bronze table:

   ```bash
   python3 read_bronze.py
   ```

6. Read the Delta Silver table:

   ```bash
   python3 read_silver.py
   ```

> **Note**: Each streaming component runs continuously and must be stopped manually (Ctrl + C).

## Fault Tolerance and Checkpointing

Spark checkpoints are used to store:

- Kafka offsets
- Streaming state

Each pipeline has its own checkpoint directory. On restart, Spark resumes processing from the last committed offsets.

## Why Kafka?

Kafka acts as a message broker that:

- Decouples data producers and consumers
- Buffers real-time data streams
- Enables scalability through partitions
- Ensures reliability through offsets and consumer groups

## Notes

- Delta Lake directories and checkpoints are generated automatically.
- `.gitkeep` files are used to preserve directory structure in Git.
- Small Parquet files are expected in streaming workloads.

## Conclusion

This project demonstrates a complete, realistic real-time data pipeline using industry-standard tools. It highlights best practices in streaming ingestion, fault tolerance, and data architecture, progressing from simple file-based streaming to a Kafka-backed real-time system.
