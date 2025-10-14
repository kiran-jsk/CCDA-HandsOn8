# Ride Sharing Analytics Using Spark Streaming and Spark SQL

A real-time analytics pipeline for a ride-sharing platform built with Apache Spark Structured Streaming. This project demonstrates streaming data ingestion, real-time aggregations, and time-based windowed analytics.

---

## Overview

This project implements a comprehensive real-time data processing pipeline that:

1. **Ingests streaming ride-sharing data** from a socket connection
2. **Parses JSON messages** into structured DataFrames with proper schema validation
3. **Performs real-time aggregations** to analyze driver performance metrics
4. **Executes time-based windowed analytics** to identify trends over sliding time windows

The system processes continuous streams of ride data including trip IDs, driver information, distances, fares, and timestamps, enabling real-time business intelligence for a ride-sharing platform.

### Key Features
- Socket-based streaming data ingestion
- JSON parsing with strict schema validation
- Real-time driver-level aggregations (total fares, average distances)
- Time-windowed analytics with watermarking for late data handling
- CSV output generation for all analytics tasks
- Fault-tolerant checkpoint management

---

## Repository Structure

```
Handson-L8-Spark-SQL_Streaming/
├── data_generator.py          # Socket server generating ride-sharing events
├── task1.py                   # Task 1: Basic streaming ingestion and parsing
├── task2.py                   # Task 2: Real-time driver-level aggregations
├── task3.py                   # Task 3: Windowed time-based analytics
├── outputs/                   # Output directory for all CSV results
│   ├── task_1/               # Parsed ride data (all fields)
│   ├── task_2/               # Driver aggregations (total_fare, avg_distance)
│   └── task_3/               # Windowed fare aggregations
├── checkpoints/              # Spark streaming checkpoints for fault tolerance
│   ├── task_1/
│   ├── task_2/
│   └── task_3/
└── README.md                 # This file
```

### File Descriptions

**`data_generator.py`**
- Generates synthetic ride-sharing events using the Faker library
- Runs a socket server on `localhost:9999`
- Sends JSON-formatted ride events every second
- Schema: `{trip_id, driver_id, distance_km, fare_amount, timestamp}`

**`task1.py`**
- Ingests streaming data from socket connection
- Parses JSON into DataFrame with schema validation
- Outputs parsed data to CSV files
- Columns: trip_id, driver_id, distance_km, fare_amount, timestamp

**`task2.py`**
- Reads streaming data and parses JSON
- Aggregates by driver_id to compute:
  - `total_fare`: SUM(fare_amount)
  - `avg_distance`: AVG(distance_km)
- Uses foreachBatch to write batched CSV outputs
- Employs "update" output mode for continuous aggregation updates

**`task3.py`**
- Converts string timestamps to TimestampType
- Implements 5-minute sliding windows (1-minute slide interval)
- Adds 1-minute watermark for late data handling
- Aggregates sum(fare_amount) per time window
- Outputs window_start, window_end, and total_fare

---

## Explanation

### Architecture Overview

The system follows a **producer-consumer architecture**:

1. **Producer (data_generator.py)**: Socket server that continuously generates ride events
2. **Consumers (task1.py, task2.py, task3.py)**: Spark Structured Streaming applications that connect to the socket, process data, and generate outputs

### Data Flow

```
Socket (localhost:9999)
    ↓
Raw Streaming Data (JSON strings)
    ↓
JSON Parsing + Schema Validation
    ↓
Structured DataFrame
    ↓
    ├─→ Task 1: Direct CSV output (append mode)
    ├─→ Task 2: Aggregation by driver_id → CSV (update mode)
    └─→ Task 3: Timestamp conversion → Windowing → CSV (update mode)
```

### Technical Implementation Details

#### Task 1: Basic Streaming Ingestion
- **Input**: Raw socket stream
- **Processing**:
  - Reads from socket using `spark.readStream.format("socket")`
  - Parses JSON using `from_json()` with predefined schema
  - Schema includes IntegerType for driver_id (matching data generator)
- **Output**: CSV files with all parsed fields
- **Trigger**: Every 5 seconds
- **Output Mode**: Append (new records only)

#### Task 2: Real-Time Aggregations
- **Input**: Parsed streaming data
- **Processing**:
  - Groups by `driver_id`
  - Computes `SUM(fare_amount)` and `AVG(distance_km)`
  - Drops null values for data quality
- **Output**: CSV files with driver-level metrics
- **Trigger**: Continuous (no explicit trigger)
- **Output Mode**: Update (modified aggregations)
- **Special Feature**: Uses `foreachBatch` for custom CSV writing with ordering

#### Task 3: Windowed Time-Based Analytics
- **Input**: Parsed streaming data with timestamps
- **Processing**:
  - Converts string timestamp to TimestampType
  - Applies 1-minute watermark for late data
  - Creates 5-minute tumbling windows sliding by 1 minute
  - Aggregates `SUM(fare_amount)` per window
- **Output**: CSV with window boundaries and total fares
- **Trigger**: Every 10 seconds
- **Output Mode**: Update (window aggregations change as data arrives)
- **Key Concept**: Overlapping windows (each event contributes to 5 different windows)

---

## Approach

### 1. Data Generation Strategy
- **Faker Library**: Generates realistic UUIDs, random driver IDs (1-100), distances (1-50 km), and fares ($5-$150)
- **Socket Server**: Binds to localhost:9999, accepts connections, and streams JSON events
- **Error Handling**: Catches broken pipes and connection resets to handle client disconnections gracefully

### 2. Schema Design
All tasks use a consistent schema to ensure data integrity:

```python
StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),    # Important: Integer, not String
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])
```

**Critical Fix**: `driver_id` must be `IntegerType()` to match the data generator's output. Using `StringType()` causes JSON parsing to fail silently, resulting in null values.

### 3. Streaming Execution Model

#### Task 1 & 2: Continuous Processing
- Process micro-batches as data arrives
- Maintain state for aggregations (Task 2)
- Write outputs incrementally

#### Task 3: Windowed Processing with Watermarking
- **Watermark**: Tracks event time progress, allows late data up to 1 minute
- **Window Function**: Creates overlapping 5-minute windows
  - Window at 10:00-10:05
  - Window at 10:01-10:06
  - Window at 10:02-10:07
  - (etc.)
- **State Management**: Spark maintains state for all active windows, purges old windows after watermark passes

### 4. Fault Tolerance
- **Checkpointing**: All tasks use checkpoint directories to track processing progress
- **Exactly-Once Semantics**: Ensures no data loss or duplication on failures
- **Recovery**: On restart, tasks resume from last committed checkpoint

### 5. Output Strategy
- **Task 1**: Standard CSV sink with headers
- **Task 2 & 3**: `foreachBatch` for custom batch processing and CSV writing
- **File Format**: CSV with headers for easy inspection and analysis

---

## Results / Outputs

### Task 1: Basic Streaming Ingestion and Parsing

**Output Location**: `outputs/task_1/`

**Sample Output**:
```csv
trip_id,driver_id,distance_km,fare_amount,timestamp
f882aaca-b60c-4100-b7f1-d1de9b23fa5f,94,49.14,42.44,2025-10-14 17:35:52
a3b9c8d7-e5f4-4a3b-9c8d-7e6f5a4b3c2d,23,15.67,78.90,2025-10-14 17:35:53
b4c9d8e7-f6a5-4b3c-9d8e-7f6a5b4c3d2e,56,32.45,105.20,2025-10-14 17:35:54
```

**Description**: Raw parsed ride events with all fields intact. Each row represents a single trip event as it arrives from the socket stream.

**Key Metrics**:
- Columns: 5 (trip_id, driver_id, distance_km, fare_amount, timestamp)
- Update Frequency: Every 5 seconds
- Output Mode: Append (cumulative data)

---

### Task 2: Real-Time Driver-Level Aggregations

**Output Location**: `outputs/task_2/batch_*/`

**Sample Output**:
```csv
driver_id,total_fare,avg_distance
18,120.93,38.83
23,456.78,25.67
45,789.12,42.91
56,234.56,18.45
```

**Description**: Aggregated metrics per driver showing cumulative earnings and average trip distance. Updates continuously as new trips arrive.

**Key Metrics**:
- Columns: 3 (driver_id, total_fare, avg_distance)
- Aggregation Level: Per driver
- Update Frequency: Real-time (updates on new data)
- Output Mode: Update (only changed drivers written)
- Sorting: Ordered by driver_id

**Business Insights**:
- Identifies top-earning drivers
- Reveals average trip distances per driver
- Enables performance-based incentive programs

---

### Task 3: Windowed Time-Based Analytics

**Output Location**: `outputs/task_3/`

**Sample Output**:
```csv
window_start,window_end,total_fare
2025-10-14T17:44:00.000-04:00,2025-10-14T17:49:00.000-04:00,15269.00
2025-10-14T17:45:00.000-04:00,2025-10-14T17:50:00.000-04:00,10773.50
2025-10-14T17:46:00.000-04:00,2025-10-14T17:51:00.000-04:00,5667.99
2025-10-14T17:47:00.000-04:00,2025-10-14T17:52:00.000-04:00,1049.02
```

**Description**: 5-minute sliding window aggregations showing total fare amounts for each time window. Windows overlap by 4 minutes (slide interval = 1 minute).

**Key Metrics**:
- Columns: 3 (window_start, window_end, total_fare)
- Window Size: 5 minutes
- Slide Interval: 1 minute
- Watermark: 1 minute (accepts late data)
- Update Frequency: Every 10 seconds
- Output Mode: Update (windows update as events arrive)

**Temporal Characteristics**:
- Each event contributes to 5 different windows
- Windows close 1 minute after watermark passes
- Timestamp format: ISO 8601 with timezone

**Business Insights**:
- Identifies peak revenue periods
- Detects temporal trends and patterns
- Enables dynamic pricing strategies
- Supports demand forecasting

---

## Execution Instructions

### Prerequisites

Ensure the following software is installed and configured:

1. **Python 3.x** (3.8 or higher recommended)
   ```bash
   python3 --version
   ```

2. **PySpark** (3.x or higher)
   ```bash
   pip install pyspark
   ```

3. **Faker** (for data generation)
   ```bash
   pip install faker
   ```

4. **Java 8 or 11** (required by Spark)
   ```bash
   java -version
   ```

### Step-by-Step Execution

#### 1. Start the Data Generator (Terminal 1)

The data generator MUST be started first as it creates the socket server.

```bash
cd /path/to/Handson-L8-Spark-SQL_Streaming
python data_generator.py
```

**Expected Output**:
```
Streaming data to localhost:9999...
```

**Keep this terminal running**. The generator will wait for client connections and start streaming data once a task connects.

#### 2. Run Individual Tasks (Terminal 2, 3, or 4)

**Important**: Wait until you see "Streaming data to localhost:9999..." before running any task.

##### Task 1: Basic Ingestion
```bash
python task1.py
```

**Expected Console Output**:
```
+--------------------+----------+-----------+-----------+-------------------+
|trip_id             |driver_id |distance_km|fare_amount|timestamp          |
+--------------------+----------+-----------+-----------+-------------------+
|f882aaca-b60c-4...|94        |49.14      |42.44      |2025-10-14 17:35:52|
+--------------------+----------+-----------+-----------+-------------------+
```

**Output Files**: `outputs/task_1/part-*.csv`

##### Task 2: Driver Aggregations
```bash
python task2.py
```

**Expected Console Output**: None (writes directly to CSV)

**Output Files**: `outputs/task_2/batch_*/part-*.csv`

##### Task 3: Windowed Analytics
```bash
python task3.py
```

**Expected Console Output**:
```
+-------------------+-------------------+----------+
|window_start       |window_end         |total_fare|
+-------------------+-------------------+----------+
|2025-10-14 17:44:00|2025-10-14 17:49:00|15269.00  |
+-------------------+-------------------+----------+
```

**Output Files**: `outputs/task_3/part-*.csv`

#### 3. Verify Outputs

Check that CSV files are being generated:

```bash
ls -lh outputs/task_1/
ls -lh outputs/task_2/
ls -lh outputs/task_3/
```

View sample output:

```bash
head outputs/task_1/part-*.csv | head -5
head outputs/task_2/batch_0/part-*.csv | head -5
head outputs/task_3/part-*.csv | head -5
```

#### 4. Stop Execution

To stop any task:
- Press `Ctrl+C` in the respective terminal

To stop all tasks and generator:
- Press `Ctrl+C` in each terminal
- Optionally, kill any lingering processes:
  ```bash
  pkill -f "python task"
  pkill -f "python data_generator"
  ```

### Running Multiple Tasks Simultaneously

You can run multiple tasks at the same time (requires separate terminals):

```bash
# Terminal 1
python data_generator.py

# Terminal 2
python task1.py

# Terminal 3
python task2.py

# Terminal 4
python task3.py
```

All tasks will receive the same streaming data from the generator.

---

## Prerequisites

### System Requirements
- **Operating System**: Linux, macOS, or Windows (with WSL recommended)
- **RAM**: Minimum 4GB, 8GB+ recommended for optimal performance
- **Disk Space**: At least 500MB free for outputs and checkpoints
- **Network**: Localhost connectivity (port 9999 must be available)

### Software Dependencies

| Software | Version | Purpose | Installation |
|----------|---------|---------|--------------|
| Python | 3.8+ | Runtime environment | `brew install python3` (macOS) or [python.org](https://python.org) |
| PySpark | 3.x | Streaming framework | `pip install pyspark` |
| Faker | Latest | Data generation | `pip install faker` |
| Java | 8 or 11 | Spark dependency | `brew install openjdk@11` (macOS) |

### Environment Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd Handson-L8-Spark-SQL_Streaming
   ```

2. **Create virtual environment (recommended)**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install pyspark faker
   ```

4. **Verify Java installation**:
   ```bash
   java -version
   # Should show version 8 or 11
   ```

5. **Create output directories** (if not exists):
   ```bash
   mkdir -p outputs/task_1 outputs/task_2 outputs/task_3
   mkdir -p checkpoints/task_1 checkpoints/task_2 checkpoints/task_3
   ```

### Port Availability

Ensure port 9999 is not in use:
```bash
lsof -i :9999  # Should return nothing
netstat -an | grep 9999  # Should return nothing
```

If port 9999 is occupied, you can modify the port in both `data_generator.py` and all task files.

---

## Errors and Resolutions

### Common Errors and Solutions

#### 1. **Connection Refused Error**

**Error Message**:
```
java.net.ConnectException: Connection refused
```

**Cause**: The data generator is not running, or task started before generator was ready.

**Solution**:
1. Verify data generator is running:
   ```bash
   lsof -i :9999
   # Should show Python process listening
   ```
2. Stop the task (Ctrl+C)
3. Start data generator first: `python data_generator.py`
4. Wait for "Streaming data to localhost:9999..."
5. Then start the task: `python task1.py`

**Verification**:
```bash
netstat -an | grep 9999 | grep LISTEN
# Should show: tcp4  0  0  127.0.0.1.9999  *.*  LISTEN
```

---

#### 2. **No Output Files Generated**

**Symptom**: Tasks run without errors, but `outputs/` directories remain empty or only contain headers.

**Cause**: Schema mismatch between data generator and task schema definition.

**Solution**:
Ensure `driver_id` is defined as `IntegerType()` in all task files:

```python
# CORRECT
StructField("driver_id", IntegerType(), True)

# INCORRECT (causes silent parsing failure)
StructField("driver_id", StringType(), True)
```

**Verification**:
1. Check task1.py, task2.py, task3.py line 11-12
2. Restart the task after fixing schema
3. Clear old checkpoints:
   ```bash
   rm -rf checkpoints/task_1
   rm -rf outputs/task_1/*
   ```

---

#### 3. **Checkpoint Conflicts**

**Error Message**:
```
org.apache.spark.sql.streaming.StreamingQueryException:
Checkpoint metadata is incompatible with current schema
```

**Cause**: Schema changed but checkpoint from old run still exists.

**Solution**:
Clear checkpoint directories and restart:

```bash
# For Task 1
rm -rf checkpoints/task_1
rm -rf outputs/task_1/*

# For Task 2
rm -rf checkpoints/task_2
rm -rf outputs/task_2/*

# For Task 3
rm -rf checkpoints/task_3
rm -rf outputs/task_3/*
```

**Then restart the task**.

---

#### 4. **Out of Memory Error**

**Error Message**:
```
java.lang.OutOfMemoryError: Java heap space
```

**Cause**: Insufficient memory allocated to Spark driver or executor.

**Solution**:
Increase JVM heap size by setting environment variables:

```bash
export SPARK_DRIVER_MEMORY=4g
export PYSPARK_DRIVER_PYTHON_OPTS="--driver-memory 4g"
```

Or modify SparkSession creation in task files:

```python
spark = SparkSession.builder \
    .appName("TaskName") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

---

#### 5. **Port Already in Use**

**Error Message**:
```
OSError: [Errno 48] Address already in use
```

**Cause**: Another process is using port 9999.

**Solution**:

Find and kill the process:
```bash
lsof -ti :9999 | xargs kill -9
```

Or change the port in all files:

In `data_generator.py`:
```python
start_streaming(host="localhost", port=9998)  # Changed from 9999
```

In all task files:
```python
.option("port", 9998)  # Changed from 9999
```

---

#### 6. **Faker Module Not Found**

**Error Message**:
```
ModuleNotFoundError: No module named 'faker'
```

**Cause**: Faker library not installed.

**Solution**:
```bash
pip install faker
# Or with specific version
pip install faker==20.0.0
```

**Verification**:
```bash
python -c "import faker; print(faker.__version__)"
```

---

#### 7. **Data Generator Disconnects Immediately**

**Symptom**: Generator shows "Client disconnected" immediately after task connects.

**Cause**: Task crashes or exits before streaming starts.

**Solution**:
1. Check task logs for errors
2. Verify schema correctness
3. Ensure checkpoint directories are writable
4. Check Java/Spark compatibility

**Debug Mode**:
Add logging to see what's happening:
```python
spark.sparkContext.setLogLevel("INFO")
```

---

#### 8. **CSV Files Have Only Headers**

**Symptom**: Output CSV files exist but contain only header row, no data.

**Cause**:
- JSON parsing failure due to schema mismatch
- No data received from socket
- Trigger interval too long

**Solution**:

1. **Verify data flow**:
   ```bash
   # Check if data generator is sending data
   nc localhost 9999
   # Should see JSON messages
   ```

2. **Check schema alignment**: Ensure all data types match between generator and task

3. **Reduce trigger interval** for faster output:
   ```python
   .trigger(processingTime="2 seconds")
   ```

4. **Add console output** to debug:
   ```python
   parsed_data.writeStream.format("console").start()
   ```

---

#### 9. **Task 3 Windows Not Updating**

**Symptom**: Windowed aggregations show initial windows but don't update.

**Cause**: Watermark prevents updates to old windows.

**Solution**:
This is expected behavior. Windows close 1 minute after watermark passes. To see updates:
- Wait for watermark to advance (at least 1 minute of data)
- Check that timestamps in data are current (not historical)
- Reduce watermark for testing:
  ```python
  .withWatermark("event_time", "10 seconds")
  ```

---

### Debugging Tips

#### Enable Detailed Logging
Add to task files:
```python
spark.sparkContext.setLogLevel("INFO")  # Or "DEBUG" for verbose
```

#### Monitor Streaming Queries
Check query progress:
```python
query.lastProgress  # View last batch progress
query.status  # View current status
```

#### Validate Data Generator Output
Test socket manually:
```bash
nc localhost 9999
# Should display JSON messages every second
```

#### Check File Permissions
Ensure write permissions:
```bash
chmod -R 755 outputs/ checkpoints/
```

#### Inspect Checkpoint Metadata
View checkpoint state:
```bash
cat checkpoints/task_1/metadata
cat checkpoints/task_1/offsets/*
```

---

### Getting Help

If issues persist:

1. Check Spark logs in console output
2. Verify all prerequisites are met
3. Ensure data generator is continuously running
4. Clear all checkpoints and outputs for a fresh start
5. Review the exact error message and stack trace
6. Check Spark documentation: [spark.apache.org/docs/latest/structured-streaming-programming-guide.html](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

---

## Additional Notes

### Performance Optimization
- For production use, consider increasing trigger intervals to reduce overhead
- Adjust parallelism with `.option("maxFilesPerTrigger", N)`
- Use Parquet instead of CSV for better performance

### Scaling Considerations
- Current implementation is single-node
- For distributed deployment, replace socket source with Kafka or other distributed sources
- Configure Spark cluster settings for multi-node execution

### Data Quality
- Task 2 includes `.dropna()` to filter incomplete records
- Consider adding data validation and cleansing logic
- Implement alerting for anomalous patterns

---

## License

This project is for educational purposes as part of the CCDA course assignment.

---

**Last Updated**: October 14, 2025
