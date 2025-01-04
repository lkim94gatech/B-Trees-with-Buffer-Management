# BTreeDB: A Lightweight Persistent B-Tree with Buffer Management

**BTreeDB** is a lightweight, persistent B-Tree implementation designed to emulate core database management system functionalities. It integrates **Buffer Management**, **Slotted Pages**, and **B-Tree Structures** for efficient data retrieval, insertion, and deletion. The project also emphasizes multi-threading, concurrency control, and the ability to maintain data persistence across sessions.

---

## Key Features

### 1. **B-Tree Implementation**
- Supports **dynamic insertion**, **deletion**, and **search** operations.
- Implements node splitting for both **leaf** and **inner nodes**.
- Handles multi-level B-Trees with robust parent-child relationships.

### 2. **Buffer Management**
- Utilizes a **Least Recently Used (LRU)** eviction policy.
- Ensures efficient in-memory page management with **multi-threading support**.
- Provides persistent storage using slotted pages to store tuples dynamically.

### 3. **Persistence**
- Ensures data is stored on disk and can be retrieved across program executions.
- Implements a **Storage Manager** for reading and writing slotted pages.

### 4. **Testing Framework**
- Includes a suite of tests to validate core functionality:
  - **Insertion** into empty and non-empty trees.
  - **Splitting** nodes during insertion.
  - **Lookup** operations on various tree states.
  - **Deletion** of keys and their verification.
  - **Persistence testing** to validate data integrity across program restarts.

---

## How to Build and Run

### Prerequisites
- A modern C++ compiler supporting **C++17** or later.
- Compatible build tools like `g++` or `clang++`.

### Compilation
```bash
g++ -std=c++17 -pthread -o btreedb main.cpp
