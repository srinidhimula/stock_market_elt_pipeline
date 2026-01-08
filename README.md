# Stock Market ELT Platform

## Project Overview
This project implements a cloud-based ELT data platform that ingests daily stock market data from a public API, loads it into a cloud data warehouse, transforms it using dbt, and orchestrates workflows with an Airflow-style scheduler.

## Tech Stack
- Python (data ingestion)
- Google Cloud Platform (BigQuery)
- dbt (data transformations)
- Airflow / Prefect (orchestration)
- GitHub Actions (CI/CD)

## Project Goals
- Build automated ingestion pipelines
- Apply analytics engineering best practices with dbt
- Orchestrate workflows with a scheduler
- Deploy and test pipelines using GitHub Actions

## Planned Pipeline
Stock API → Python → BigQuery → dbt → Orchestrator → Analytics tables

## Project Structure
- ingestion/: API ingestion scripts
- orchestration/: workflow definitions
- dbt/: transformation layer
- infrastructure/: cloud & deployment configs
- docs/: design & diagrams

## Status
Phase 0 – project initialization
