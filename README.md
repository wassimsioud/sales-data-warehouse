# Sales Data Warehouse ETL Pipeline

ETL pipeline for sales data warehouse with dimensional modeling and KPI dashboards. This project transforms raw CRM and ERP data into a dimensional model following the Medallion Architecture (Bronze → Silver → Gold).

## 📋 Overview

This data warehouse solution integrates data from multiple sources (CRM and ERP systems) and transforms it into a dimensional model optimized for analytics and reporting. The pipeline processes customer, product, and sales data through three layers:

- **Bronze Layer**: Raw source data from CRM and ERP systems
- **Silver Layer**: Cleaned and standardized data
- **Gold Layer**: Dimensional model (star schema) with fact and dimension tables

## 🏗️ Architecture

### Data Model

**Dimension Tables:**
- `dim_customers` - Customer master data with demographics and location
- `dim_products` - Product master data with categories and attributes

**Fact Tables:**
- `fact_sales` - Sales transactions with metrics (quantity, amount, cost, profit)

### Project Structure
