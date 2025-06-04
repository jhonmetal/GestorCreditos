# ELT Process for Credit Approval and Monitoring

## Overview

A leading financial company specializing in offering credit to individuals and SMEs faces a complex and urgent challenge: optimizing and automating the real-time approval, monitoring, and tracking of loans. The company processes over 100,000 credit applications each month, which requires a robust ELT (Extract, Load, Transform) pipeline for efficient decision-making.

This project builds an ELT pipeline for processing and analyzing financial data, with a focus on improving the speed and accuracy of credit approval and monitoring.

## Analytics Process Overview

The Lambda Architecture is a deployment model for data processing that organizations use to combine a traditional batch pipeline with a fast real-time stream pipeline for data access.

The ELT process is divided into several stages, primarily utilizing the **Bronze**, **Silver**, and **Gold** layers from Medallion architecture to refine raw data through various transformations and aggregations. The stages include:

1. **Bronze Layer (Raw Data)**: Ingest raw data from CSV files into the system.
2. **Silver Layer (Cleaned Data)**: Transform and clean the data for better analysis, filtering and combining the raw data.
3. **Gold Layer (Aggregated Data)**: Finalized and aggregated data ready for reporting and decision-making.

## ER Diagram

The ER Diagram represents the high-level data architecture for the ELT process, highlighting the entities and their relationships.

![ER Diagram for ELT](./assets/ETL-erd.svg)

## Tables Overview

The following tables are created and processed throughout the ELT pipeline:

### Bronze Layer
- `transactions`: Contains raw transaction data.
- `accounts`: Contains raw account data.
- `customers`: Contains raw customer data.

### Silver Layer
- `large_transactions`: Identifies large transactions above a certain threshold.
- `transactions_master`: Fact transaction data for analysis.

### Gold Layer
- `account_master`: Tracks balance usage by account.
- `top_amount_yearly_accounts`: Table tracking the top accounts by yearly transaction volume.
- `top_monthly_accounts`: Table for tracking the top accounts by monthly transaction volume.

---
