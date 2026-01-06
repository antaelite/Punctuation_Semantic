# Tucker et al. 2003 Punctuation Semantics Implementation

## Overview

This implementation replicates the key concepts from **Tucker et al. (2003) "Exploiting Punctuation Semantics in Continuous Data Streams"** using Apache Flink.

## Implementation Structure

### 1. Data Models (`src/main/java/org/example/punctuation/model/`)

#### `SensorReading.java`
- Implements the paper's temperature sensor schema: `<sid, hour, minute, curtmp>`
- Represents continuous temperature readings from warehouse sensors
- Implements `StreamElement` interface for polymorphic stream processing

#### `HourlyPunctuation.java`
- Represents punctuations following Tucker et al. 2003 semantics
- Semantics: "No more data for sensor `sid` for hour `hour` will arrive"
- Enables state cleanup at hour boundaries

#### `StreamElement.java`
- Common interface for both data and punctuation elements
- Allows operators to process mixed streams polymorphically

### 2. Data Sources (`src/main/java/org/example/punctuation/source/`)

#### `TemperatureSensorSource.java`
- **Intelligent Sensor** that embeds punctuations in the stream
- Generates continuous temperature readings (15-35°C range)
- Emits **hourly punctuations** at hour boundaries for all sensors
- Follows Section 7.3 evaluation methodology:
  - Multiple sensors (S1, S2, S3)
  - Configurable duration (hours)
  - Configurable reading frequency

### 3. Operators (`src/main/java/org/example/punctuation/operators/`)

#### `NaiveUnionOperator.java`
**Baseline: WITHOUT Punctuation Optimization**
- Implements duplicate elimination (Union operator from Section 7.2.1)
- Keeps ALL unique tuples in state indefinitely
- **Ignores punctuations** - does not use them for optimization
- Results in **unbounded state growth** (memory leak)
- Expected behavior: Linear growth pattern

#### `PunctuatedUnionOperator.java`
**Optimized: WITH Punctuation Optimization**
- Implements duplicate elimination WITH punctuation semantics
- Follows Tucker et al. 2003 invariants:
  - **KEEP Invariant**: When punctuation arrives for hour X, purge all state entries matching (sid, hour X)
  - **PASS Invariant**: Output aggregated results when punctuation arrives
- Results in **bounded state** with sawtooth pattern
- Expected behavior: State grows during hour, drops to near-zero at hour boundaries

### 4. Metrics (`src/main/java/org/example/punctuation/metrics/`)

#### `StateMetrics.java`
- Container for tracking state size over time
- Used to replicate Figure 2(a) from the paper

#### `MetricsCollector.java`
- Collects and exports metrics to CSV
- Enables visualization of state size patterns

### 5. Main Job (`src/main/java/org/example/punctuation/job/`)

#### `TuckerReplicationJob.java`
- Main entry point demonstrating both approaches side-by-side
- Configuration:
  - 3 sensors (S1, S2, S3)
  - 5 hours simulation (adjustable)
  - Multiple readings per minute
- Runs both naive and punctuated operators in parallel for comparison

## Key Concepts Implemented

### 1. Punctuation Semantics
Following Tucker et al. 2003, a punctuation is a **predicate** embedded in the stream that asserts:
> "No more tuples matching this predicate will arrive"

In our implementation:
- `HourlyPunctuation(sid="S1", hour=5)` means: "No more readings for sensor S1 during hour 5 will arrive"

### 2. KEEP Invariant
From Section 4.2 of the paper:
> "Tuples that match the punctuation no longer need to be retained"

Our implementation:
```java
// When punctuation arrives for (sid, hour):
for (SensorReading reading : seenTuples.keys()) {
    if (reading.getSid().equals(punc.getSid()) && 
        reading.getHour() == punc.getHour()) {
        seenTuples.remove(reading);  // Safe to purge
    }
}
```

### 3. PASS Invariant
From Section 4.2 of the paper:
> "The operator can produce output when punctuation arrives"

Our implementation:
```java
if (element.isPunctuation()) {
    // Process and output results for completed hour
    out.collect("Completed hour " + punc.getHour() + "...");
}
```

## Expected Results

### Naive Operator (Figure 2a - dashed line)
- State size grows **linearly** without bound
- Memory consumption increases continuously
- Represents the problem that punctuation semantics solves

### Punctuated Operator (Figure 2a - solid line)
- State size shows **sawtooth pattern**
- State grows during each hour
- State drops to near-zero at hour boundaries (after punctuation)
- Memory remains bounded

## Running the Implementation

### Option 1: Using IntelliJ IDEA
1. Open the project in IntelliJ
2. Run `TuckerReplicationJob.java`
3. Observe console output showing state size changes

### Option 2: Using Maven (if available)
```bash
mvn clean package
mvn exec:java -Dexec.mainClass="org.example.punctuation.job.TuckerReplicationJob"
```

### Option 3: Using Flink CLI
```bash
mvn clean package
flink run target/punctuation-semantics-1.0-SNAPSHOT.jar
```

## Console Output Interpretation

You should see output like:
```
SOURCE: End of hour 0 - emitting punctuations
SOURCE: Punctuation emitted for sensor=S1, hour=0
NAIVE-UNION [S1-0]: State size = 120, Total tuples = 120
PUNCTUATED-UNION [S1-0]: Received punctuation for hour 0
PUNCTUATED-UNION [S1-0]: Purged 120 tuples. State size now = 0
```

**Key observations:**
- NAIVE state keeps growing
- PUNCTUATED state drops to 0 after each hour
- This demonstrates the memory optimization achieved by punctuation semantics

## Differences from Original Paper

1. **Simplified schema**: We use fixed sensors instead of complex warehouse topology
2. **Shorter duration**: 5 hours instead of 60 (configurable)
3. **Modern framework**: Flink instead of STREAM/NiagaraCQ
4. **Simplified aggregation**: Count instead of complex MAX/AVG operations

## References

- Tucker, P. A., Maier, D., Sheard, T., & Fegaras, L. (2003). Exploiting punctuation semantics in continuous data streams. *IEEE Transactions on Knowledge and Data Engineering*, 15(3), 555-568.

## File Structure
```
src/main/java/org/example/punctuation/
├── model/
│   ├── StreamElement.java          # Common interface
│   ├── SensorReading.java          # Data tuples
│   └── HourlyPunctuation.java      # Punctuation markers
├── source/
│   └── TemperatureSensorSource.java # Intelligent sensor
├── operators/
│   ├── NaiveUnionOperator.java     # WITHOUT punctuation
│   ├── PunctuatedUnionOperator.java # WITH punctuation
│   └── MetricsCollector.java        # Metrics export
├── metrics/
│   └── StateMetrics.java            # State tracking
└── job/
    └── TuckerReplicationJob.java    # Main entry point
```
