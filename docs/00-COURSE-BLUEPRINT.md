# Data Engineering with PySpark — Course Blueprint

**Working title:** *From Backend to Big Data: Data Engineering with PySpark*
**Audience:** Java / backend developers moving into data engineering
**Format:** Single-file HTML course player (established architecture: MODS array, Fraunces / JetBrains Mono, warm paper palette, dual-path labs)
**Labs:** 100% browser-based — no installs (see `03-LAB-ENGINE-SPEC.md`)
**Authoring engine:** Claude Code (see `01-CLAUDE.md` and `02-AUTHORING-WORKFLOW.md`)
**Scale:** 8 tracks · 36 modules

---

## 1. Domain Anchor: NimbusMart

A neutral, globally relatable domain: **NimbusMart**, a mid-size online marketplace. Every module's examples, datasets, labs, and the capstone live in this one world.

**The cast of data sources (introduced in A1, reused everywhere):**

| Source | Shape | Teaches |
|---|---|---|
| `orders` clickstream events (JSON) | semi-structured, high volume | ingestion, schema drift, streaming concepts |
| `orders` OLTP export (CSV/Parquet) | relational | batch loads, CDC, incremental processing |
| `customers` master (CSV, messy) | dirty reference data | cleansing, dedup, SCD2 |
| `products` catalog (nested JSON) | nested/arrays | explode, struct handling |
| `payments` + `fraud_scores` feed | joined risk data | joins, the threshold concept |
| `couriers` delivery pings | event-time data | windows, late data |

**Recurring threshold concept:** `FRAUD_REVIEW_THRESHOLD = 0.80`
Orders with a fraud score **below** 0.80 auto-fulfill; at or above it they land in a human review queue. This constant appears first in A1 as a business rule, resurfaces in SQL labs (C2), joins (E3), window functions (E4), quality gates (G2), and is the spine of the capstone (Track H). It plays the same role `AUTO_APPROVE_THRESHOLD = 0.85` plays in the Angular course — one number the student meets 15 times, in deeper contexts each time.

---

## 2. Recurring Analogy System: "The Freight Line"

Every abstract DE concept maps to one consistent warehouse-logistics metaphor (the same job "The Lakeview Build" does for AI-DLC). The analogy is *itself* NimbusMart's fulfillment warehouse, so the metaphor and the domain reinforce each other.

| DE concept | Freight Line analogy |
|---|---|
| Bronze layer | **Receiving dock** — accept every truck, log everything, reject nothing |
| Silver layer | **QC station** — unpack, inspect, standardize, discard damaged goods (with a record) |
| Gold layer | **Showroom** — arranged for the customer, not the warehouse |
| Partitioning | **Aisle + bin labels** — find without walking the whole warehouse |
| Shuffle | **Cross-dock transfer** — the expensive forklift trip between zones |
| Broadcast join | **Pocket reference card** — small list every picker carries vs. walking to the master board |
| Skew | **One aisle everyone crowds into** on sale day |
| Idempotency | **Re-scanning a barcode** — same result, no duplicate stock |
| Backfill | **Inventory recount** of past weeks without stopping today's shipments |
| Data contract | **The pallet spec** — suppliers who violate it get rejected at the dock |
| Orchestrator (Airflow) | **Shift supervisor's clipboard** — who does what, in what order, and what happens when someone calls in sick |

**Java-dev bridge callouts:** every module includes a `☕ For the Java Dev` box mapping the new idea onto something they already own:

- Spark transformations ↔ `Stream.map().filter()` — lazy until a terminal operation
- Driver/executors ↔ `main()` thread + a managed thread pool, across machines
- DataFrame schema ↔ POJO + Jackson, but columnar
- Catalyst optimizer ↔ JIT: you write intent, the runtime rewrites it
- UDF serialization cost ↔ crossing the JNI boundary
- Airflow DAG ↔ Jenkins pipeline for data
- Delta MERGE ↔ JPA upsert semantics, at billions of rows

---

## 3. Track & Module Map (8 tracks · 36 modules)

### Track A — Foundations: From Backend to Data (5 modules)
| # | Module | Core idea | Lab tier* |
|---|---|---|---|
| A1 | Welcome to NimbusMart: Why Data Engineering | OLTP vs OLAP; why the orders DB can't answer analytics questions; meet `FRAUD_REVIEW_THRESHOLD` | T3 story-sim |
| A2 | The Freight Line: Anatomy of a Data Platform | ingest → transform → serve; Bronze/Silver/Gold preview; batch vs streaming at 10,000 ft | T3 |
| A3 | Files That Scale | CSV → JSON → Parquet/Avro/ORC; row vs columnar; ☕ Jackson serialization vs columnar encoding | T2 format explorer |
| A4 | Partitioning, Compression & the Small-Files Problem | directory layout as an index; why 1M tiny files kills a cluster | T2 |
| A5 | Batch vs Streaming | latency/throughput/cost triangle; micro-batch as the pragmatic middle | T3 |

### Track B — Data Modeling & the Lakehouse (4 modules)
| B1 | From JPA Entities to Star Schemas | facts vs dimensions; normalization is for writes, stars are for reads | T1 SQL |
| B2 | Slowly Changing Dimensions | SCD1 vs SCD2; the customer who moved cities mid-quarter | T1 |
| B3 | Medallion Architecture | Bronze/Silver/Gold contracts; what belongs in each layer and what never does | T3 |
| B4 | Warehouse, Lake, Lakehouse | history in 10 minutes; table formats (Delta/Iceberg) as "Git for Parquet" | T3 |

### Track C — The Working Toolkit (4 modules)
| C1 | Python for Java Developers, Compressed | duck typing, comprehensions, no semicolons; only what Spark needs (condensed from the standalone course) | T1 Pyodide |
| C2 | SQL for Analytics | CTEs, window functions, aggregations; fraud-review queue in pure SQL | T1 SQL (in-browser) |
| C3 | Pandas as the Gateway | DataFrame mental model on one machine before distributing it | T1 Pyodide |
| C4 | Thinking in Data Quality | nulls, dupes, drift; quality as an engineering property, not a QA phase | T1 |

### Track D — Spark Core Mental Model (5 modules)
| D1 | Why Spark: One JVM Is Not Enough | scale-up vs scale-out; ☕ your `ExecutorService` becomes a cluster | T3 |
| D2 | Driver, Executors, Cluster Managers | runtime anatomy; where your code actually runs | T2 anatomy sim |
| D3 | DataFrames & Lazy Evaluation | transformations vs actions; ☕ Java Streams' laziness, distributed | T2 SparkSim |
| D4 | Jobs, Stages, Tasks & the Shuffle | reading the DAG; why `groupBy` draws a stage boundary | T2 DAG visualizer |
| D5 | Catalyst & Tungsten | logical → optimized → physical plan; predicate pushdown live | T2 plan explorer |

### Track E — PySpark in Practice (6 modules)
| E1 | Reading & Writing at Scale | sources, explicit schemas, why `inferSchema` lies to you | T2 |
| E2 | Transformations Deep-Dive | select/filter/withColumn/groupBy/agg on NimbusMart orders | T2 SparkSim |
| E3 | Joins: Broadcast, Sort-Merge & Skew | the fraud-score join; salting the hot key (the mega-seller problem) | T2 + T3 skew sim |
| E4 | Window Functions in Spark | ranking couriers, running revenue, deduplicating events | T2 |
| E5 | UDFs vs Built-ins | serialization tax; pandas UDFs; ☕ the JNI-boundary analogy | T2 + T3 perf sim |
| E6 | Incremental Processing & Delta Lake | MERGE, time travel, schema evolution; CDC from the OLTP export | T2 |

### Track F — Pipelines & Orchestration (4 modules)
| F1 | From Script to Pipeline | idempotency, retries, checkpoints; the re-scanned barcode | T2 |
| F2 | Airflow DAGs | operators, sensors, scheduling; the shift-supervisor's clipboard | T2 DAG builder |
| F3 | Backfills, Catch-up & SLAs | reprocessing history safely; `execution_date` mental model | T2 |
| F4 | CI/CD for Data Pipelines | environments, testing pipelines (chispa-style assertions), deploy gates | T1 + T3 |

### Track G — Quality, Governance & Ops (4 modules)
| G1 | Data Contracts & Schema Evolution | the pallet spec; producer/consumer negotiation; additive vs breaking | T1 |
| G2 | Validation Gates | expectations at the Bronze→Silver boundary; quarantine tables; the fraud threshold as a *tested* rule | T2 |
| G3 | Lineage, Cataloging & Observability | answering "where did this number come from?"; freshness SLOs | T3 |
| G4 | Performance & Cost Tuning Basics | partitions count, caching, file sizing; reading the Spark UI | T2 + T3 |

### Track H — Capstone: The NimbusMart Platform (4 modules)
| H1 | Capstone Brief | all six sources → executive dashboard; acceptance criteria; `FRAUD_REVIEW_THRESHOLD` end-to-end | — |
| H2 | Build Bronze → Silver | ingestion, contracts, quality gates, quarantine | T2 guided build |
| H3 | Build Gold | SCD2 customer dim, order fact, fraud review queue mart | T2 guided build |
| H4 | Orchestrate, Document, Demo | DAG for the whole platform; lineage doc; the dashboard lights up | T2 + T3 finale |

\* Lab tiers defined in `03-LAB-ENGINE-SPEC.md`: **T1** = real in-browser execution (Pyodide/SQL), **T2** = SparkSim interactive engine, **T3** = scripted simulation/animation for cluster-scale behavior.

---

## 4. Module Anatomy (per module, standard MODS entry)

1. **Cold open** — a NimbusMart incident or question (2–3 sentences, narrative)
2. **Concept** — the teaching core; Freight Line analogy panel; 1–2 diagrams (inline SVG)
3. **☕ For the Java Dev** — bridge callout box
4. **Lab — Path 1: Understand It** — in-browser interactive (tier per module map)
5. **Lab — Path 2: Build It with AI** — copy-paste Claude Code prompt that scaffolds the equivalent real PySpark project locally, for students who want to go beyond the browser
6. **Check** — 3–5 questions (mix of MCQ + predict-the-output)
7. **Field Notes** — production war-story tied to the concept (small-files incident, skew outage, backfill horror)

## 5. Learning Outcomes (course-level)

By the end, a Java/backend developer can:
1. Explain OLTP vs OLAP, Medallion Architecture, and lakehouse table formats to a colleague
2. Read and predict a Spark physical plan; identify shuffle boundaries and skew before running the job
3. Write idiomatic PySpark: schemas, transformations, joins, windows, Delta MERGE
4. Design an idempotent, backfillable pipeline with quality gates and a quarantine path
5. Ship the NimbusMart capstone: six sources → Bronze/Silver/Gold → orchestrated DAG → dashboard
