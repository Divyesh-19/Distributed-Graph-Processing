
# Distributed Graph Processing using Apache Spark

This project demonstrates how to efficiently process large-scale graphs using a distributed system—**Apache Spark**—as part of the **Big Data Processing (IE494)** course. The implementation is based on the research by *Prof. Rajshekhar Sunderraman* and *Dr. Janani Balaji*.

## 👥 Authors

- Divyesh Ramani - 202201241  
- Manan Patel - 202201310  
- Supervisor: Prof. P.M. Jat

## 📌 Objective

To implement and test a scalable graph processing approach using Apache Spark that enables subgraph querying by distributing computations across multiple nodes. The system supports parsing large graphs, segmenting query graphs, and performing iterative subgraph matching.

## 📁 Files

- `Distributed_Graph_Processing.ipynb`: Full implementation of graph query processing in Apache Spark (Google Colab notebook).
- `T44_Project_Report.pdf`: Detailed project report covering methodology, design, testing, and analysis.

## 🧠 Problem and Solution

Processing and querying large-scale graph data using a centralized system is inefficient and memory-bound. The solution leverages Spark’s distributed in-memory processing to decompose queries and process them in parallel across the cluster, enhancing performance and scalability.

## 🛠️ Features and Implementation

### 1. Node and Edge File Parsing
- Reads input files to construct a graph.
- Nodes: `NodeLabel_NodeType` format → stored in a dictionary.
- Edges: `SourceNode_TargetNode_EdgeLabel` format → stored in `temp_graph` as incoming/outgoing dictionaries.

### 2. Graph & Query Construction
- Converts parsed data into Spark RDDs for the full graph and query graph.
- Persists RDDs in memory for faster iteration.

### 3. Query Graph Segmentation
- Divides the query graph into segments.
- Start and end nodes include full edge context; intermediate nodes are split into individual edges.

### 4. Valid Candidate Initialization
- Starts with all graph nodes as possible match candidates.

### 5. Iterative Subgraph Matching
- Filters valid candidates using node labels and edge structure.
- Uses broadcasting and neighbor expansion to refine matches iteratively.

### 6. Result Collection
- Collects and displays the final matched subgraphs.

## ✅ Testing Strategy

Includes unit tests for:
- Node and edge parsing
- Graph and query RDD construction
- Query segmentation logic
- Iterative subgraph matching
- Edge cases: malformed data, empty inputs, cyclic graphs

## 📊 Sample Output

Graph subgraph matches are printed with tracked paths, demonstrating accurate query resolution.

## 🔗 Reference

- Janani Balaji and Rajshekhar Sunderraman, “Distributed Graph Path Queries Using Spark,” *IEEE COMPSAC 2016*.

## ▶️ How to Run

1. Clone the repo or open the Colab notebook:  
   [Open on Google Colab](https://colab.research.google.com/drive/1d2crbUNXMrL-buUkMa2cr1S4ivGXKyPB?usp=sharing)

2. Upload the node and edge files.

3. Follow the notebook's step-by-step cells to:
   - Parse the graph
   - Load the query
   - Execute distributed subgraph search
