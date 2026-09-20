# Master Spark Architecture

This repository is the stable reference for the approved **Master Spark Architecture — Version 1**.

## Core purpose

The architecture is designed to build a systems-level mental model of Spark by connecting:

**Code → Logical Plan → Optimization → Physical Plan → Execution → Evidence → Diagnosis → Optimization**

The reference covers Spark application architecture, planning, execution, scheduling, stages, tasks, partitions, shuffle, memory, AQE, storage, observability, and performance diagnosis.

## Documents

- [Master Spark Architecture](docs/MASTER_SPARK_ARCHITECTURE.md)

## Learning rule

Future Spark deep-dives should locate themselves inside this master architecture rather than creating disconnected mental models.
