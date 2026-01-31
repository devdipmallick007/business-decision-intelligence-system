# business-decision-intelligence-system

## Overview

The **Business Decision Intelligence System (BDIS)** is a modular, production-oriented data intelligence platform designed to transform raw enterprise data into reliable, validated, and decision-ready datasets. The system focuses on **data correctness, schema enforcement, and pipeline discipline** before any modeling or AI is applied.

This project is intentionally built with **clear separation of concerns** (fetching, validation, cleaning, feature engineering, and downstream intelligence) to support scalability, auditability, and future autonomous decision-making systems.

---

## Core Philosophy

This system follows a few non-negotiable principles:

* **Schema-first design**: Data contracts are defined explicitly before transformations.
* **Fail fast, fail loud**: Structural issues are caught early in the pipeline.
* **Validation ≠ Cleaning**: Validation checks assumptions; cleaning fixes problems.
* **Production realism**: No notebook-only logic, no silent coercions.

---

## High-Level Architecture

```
Database
   ↓
Data Fetch Layer
   ↓
Schema Validation Layer  ← (structure, keys, expectations)
   ↓
Data Cleaning Layer      ← (casting, imputation, standardization)
   ↓
Feature Engineering
   ↓
Decision / ML / Simulation Layers (future)
```

---

## Repository Structure

```
business-decision-intelligence-system/
│
├── core/
│   ├── db.py                 # Database connection & extraction
│   ├── log.py                # Centralized logging configuration
│   └── ...
│
├── validation/
│   └── schema_validator.py   # Schema & structural validation logic
│
├── schema/
│   └── schema.yml            # Declarative schema definitions (contracts)
│
├── data/
│   └── raw/                  # Raw extracted CSVs (no modification)
│
├── main.py                   # Pipeline entry point
├── pyproject.toml            # Project configuration & dependencies
└── README.md
```

---

## Schema Validation Layer (Key Concept)

The **schema validation layer** ensures that incoming data matches expectations *before* any cleaning or transformation occurs.

It validates:

* Required columns
* Primary key uniqueness
* Column presence and naming
* Basic datatype expectations (non-coercive)

❗ This layer **does not fix data**. It only reports violations.

If validation fails, the pipeline stops and logs the issue.

---

## Schema vs Data Cleaning (Important Distinction)

| Aspect   | Schema Validation              | Data Cleaning               |
| -------- | ------------------------------ | --------------------------- |
| Purpose  | Enforce contracts              | Fix data issues             |
| Action   | Check & fail                   | Transform & repair          |
| Examples | Missing columns, PK duplicates | Type casting, null handling |
| When     | Immediately after fetch        | After validation            |

---

## Branch Strategy

The project follows a disciplined branching model:

* **main** → Stable, reference-ready state
* **schema** → Active schema & validation development
* **feature/*** → Experimental or isolated features

Foundation changes (like schema rules) live in feature branches until stabilized.

---

## Current Status

* ✅ Database extraction implemented
* ✅ Centralized logging enabled
* ✅ Schema validation framework in place
* 🟡 Data cleaning layer (next)
* 🟡 Feature engineering (planned)
* 🔜 Decision intelligence / simulation layers

---

## How to Run

```bash
python main.py
```

Ensure:

* Database credentials are configured
* `schema/schema.yml` reflects expected contracts

---

## Future Roadmap

* Schema versioning (v1, v2, backward compatibility)
* Warning vs error severity levels
* Automated schema drift detection
* Feature store integration
* Autonomous agent-based decision simulation

---

## Final Note

This project is **not a demo pipeline**.
It is a foundation for **real-world, production-grade decision intelligence systems** where correctness matters more than convenience.

If schema validation passes, downstream systems can trust the data.
