# Replication Plan: Exploiting Punctuation Semantics in Continuous Data Streams

**Original Paper:** Tucker et al. (2003) [cite_start]- Exploiting Punctuation Semantics in Continuous Data Streams [cite: 3, 4]
**Target System:** Apache Flink (Java)
**Team Members:** Anta Gueye, Louis Kusno, Jassir Habba

## 1. Objective
The goal of this project is to reproduce the memory optimization techniques described in the paper using a modern streaming engine (Apache Flink). [cite_start]Specifically, we aim to demonstrate how **Punctuation Semantics** can allow unbounded stateful operators to function efficiently on infinite streams by clearing state early[cite: 10, 11].

## 2. Scope of Replication
[cite_start]We will reproduce **Example 1: Tracking Temperature in a Warehouse**[cite: 24].
* **Justification:** The paper requires an architecture that supports continuous queries and stateful operators. Flink is the modern standard for stateful stream processing and allows low-level access to state via ProcessFunctions, which is necessary to implement the custom "Keep Invariants" logic.
* **Scenario:** Temperature sensors report data continuously. We need to output the maximum temperature per hour.
* [cite_start]**Problem:** Without punctuation, a standard `Union` or `Group-By` operator maintains an infinite state (history of all temperatures) because it waits for the end of the stream[cite: 26, 27].
* [cite_start]**Solution:** We will inject "Punctuations" at the end of each hour to signal that no more data for that hour will arrive, triggering state cleanup[cite: 73, 76].

## 3. Dataset

* We will not use a static dataset but rather a Custom Synthetic Data Generator, strictly following the specifications in Section 7.3 of the paper.
* Schema: Tuples of <sid, hour, minute, currtmp>.
* Stream Behavior:
* Continuous stream of temperature readings.
* Punctuation Injection: The generator will embed a punctuation at the end of each hour, matching the paper's strategy where "sensors embed punctuations at the end of each hour".
* Volume: We will simulate the "60 hours" duration mentioned in the evaluation.

## 4. Implementation Plan (Phase 1)

### Phase 1: Stream Generation & Punctuation
* **System Setup:** Initialize Apache Flink project (Java/Scala).

* **Goal:** Implement a Flink SourceFunction that acts as the "Intelligent Sensor".
* **Logic:** It will emit SensorReading objects and, upon the transition of the hour attribute, emit a special Punctuation object. This corresponds to the paper's definition of punctuation as a predicate embedded in the stream.

### Phase 2: Operator Replication (The "Union" Operator)
* **Goal:** Reproduce the behavior of the Union operator (Duplicate Elimination) described in Section 7.2.1.
* **Baseline (Naive):** Implement a version that keeps all unique tuples in a Set state. This represents the "Unbounded stateful operator".
* **Optimized (Punctuated):** Implement the Keep Invariant. When a Punctuation arrives for Hour X:
The operator identifies state entries matching Hour X.
The operator purges these entries, as "tuples that match those punctuations no longer need be retained".

### Phase 3: Evaluation Methodology
We will validate our reproduction by replicating Figure 2(a): State Size for Union Operator.
* Metric: We will measure the number of tuples held in the Flink KeyedState over time.
* Expected Outcome:
   1. Without Punctuation: Linear growth of state size (memory leak).
   2. With Punctuation: A "sawtooth" pattern where state drops to near-zero at the end of each hour.
