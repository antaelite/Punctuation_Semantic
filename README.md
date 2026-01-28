# Punctuation Semantic Stream Processing

A Java-based stream processing project demonstrating the use of **punctuation semantics** to optimize state management in streaming applications using Apache Flink.

## Overview

This project explores how punctuation markers in data streams can help manage and bound operator state, preventing unbounded state growth in long-running stream processing applications. It uses NYC Taxi trip data to demonstrate various stream operators with punctuation-aware processing.

## Key Features

- **Punctuation-Aware Operators**: Custom stream operators that leverage punctuation markers to manage state efficiently
- **Duplicate Elimination**: State-bounded duplicate removal using punctuation semantics
- **Generic GroupBy**: Flexible aggregation operators with configurable key extraction and aggregation strategies
- **State Management**: Demonstrates how punctuation enables state cleanup without sacrificing correctness
- **Performance Analysis**: Python scripts to visualize the impact of punctuation on state size

## Architecture

### Core Components

- **StreamItem**: Wrapper class that encapsulates both data tuples and punctuation markers
- **PunctuatedIterator**: Iterator interface for processing streams with punctuation
- **PunctuationInjector**: Ingestion component that inserts punctuation markers into the data stream
- **Punctuation Model**: Represents punctuation markers with configurable strategies

### Stream Operators

- **StreamDuplicateElimination**: Removes duplicate tuples while using punctuation to bound state size
- **GenericStreamGroupBy**: Configurable aggregation operator with custom key extraction and aggregation logic
- **StreamCountSameBorough**: Counts taxi rides within the same borough
- **StreamCountDiffBorough**: Counts taxi rides between different boroughs

### Data Model

- **TaxiRide**: NYC Taxi trip data model with fields like medallion, hack license, pickup/dropoff boroughs, passenger count, etc.
- **GroupByResult**: Generic result container for group-by operations
- **BoroughCountItem**: Specialized count results for borough-based queries

## Technology Stack

- **Java 17**: Primary programming language
- **Apache Flink 1.17.1**: Stream processing framework
- **Maven**: Build and dependency management
- **Lombok**: Reduces boilerplate code
- **Jackson**: JSON data binding
- **Python 3.x**: Analysis and visualization
- **Matplotlib**: Graph generation

## Project Structure

```
punctuation-semantic/
├── src/main/java/org/example/
│   ├── Main.java                      # Application entry point
│   ├── core/                          # Core abstractions
│   │   ├── PunctuatedIterator.java
│   │   └── StreamItem.java
│   ├── ingestion/                     # Data ingestion components
│   │   ├── PunctuationInjector.java
│   │   └── TaxiDataMapper.java
│   ├── model/                         # Data models
│   │   ├── TaxiRide.java
│   │   ├── Punctuation.java
│   │   ├── AggregationStrategy.java
│   │   └── SerializableKeyExtractor.java
│   ├── operators/                     # Stream operators
│   │   ├── StreamDuplicateElimination.java
│   │   ├── GenericStreamGroupBy.java
│   │   ├── StreamGroupBy.java
│   │   ├── StreamCountSameBorough.java
│   │   └── StreamCountDiffBorough.java
│   └── utils/                         # Utility classes
│       └── GeoUtils.java
├── analysis/                          # Python analysis scripts
│   ├── graph.py                       # State size visualization
│   ├── sort.py                        # Data sorting utilities
│   └── duplicate_elimination_metrics.csv
├── pom.xml                            # Maven configuration
└── README.md                          # This file
```

## Getting Started

### Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd punctuation-semantic
```

### Run

Add the following lines in the VM options of your IDE

```
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
```

## Analysis

### Visualizing State Size Impact

The project includes Python scripts to analyze the impact of punctuation on state management:

```bash
python analysis/graph.py
```

This generates a visualization showing:

- **Processed Tuples**: Growing linearly over time
- **Current State Size**: Bounded by punctuation, preventing unbounded growth

The graph is saved to `analysis/state_size_graph.png`.

## Performance

Punctuation semantics enable:

- **Bounded State**: State size remains constant or bounded, preventing memory overflow
- **Correct Results**: Maintains correctness while allowing state cleanup
- **Scalability**: Enables long-running stream processing applications
