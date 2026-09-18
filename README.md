# E-Commerce Data Pipeline — Databricks + DAB + CI/CD

A version-controlled Bronze → Silver → Gold data pipeline deployed to Databricks via **Declarative Automation Bundles (DAB)**, promoted from TEST to PROD through **GitHub Actions CI/CD**.

## Architecture

```
         CSV Data (Git)
              │
              ▼
     ┌────────────────┐
     │  GitHub Actions │
     │  (CI/CD)        │
     └───────┬────────┘
             │ databricks bundle deploy
             │ databricks fs cp (upload CSVs)
             │ databricks bundle run
             ▼
     ┌────────────────┐
     │   Databricks    │
     │   Workflow      │
     └───────┬────────┘
             │
    ┌────────┼────────────────┐
    ▼        ▼        ▼       ▼
  Bronze   Bronze   Bronze  Bronze
  Cust.    Prod.    Orders  Items
    └────────┼────────────────┘
             ▼
          Silver
       (Clean + Transform)
             │
    ┌────────┼────────┐
    ▼        ▼        ▼
  Daily    Product  Customer
  Sales    Sales    Sales
  (Gold)   (Gold)   (Gold)
```

## Project Structure

```
├── data/                    # Sample CSVs (committed to Git, uploaded by CI/CD)
│   ├── cust_data.csv
│   ├── prod_data.csv
│   ├── ord_data.csv
│   └── orditems_data.csv
├── src/
│   ├── setup/               # Schema + Volume creation
│   ├── bronze/              # Raw ingestion (4 scripts)
│   ├── silver/              # Cleaning + transformation
│   └── gold/                # Business analytics (3 scripts)
├── resources/
│   └── ecommerce_job.yml    # DAB workflow definition
├── databricks.yml           # Bundle config with dev/test/prod targets
└── .github/workflows/
    └── databricks-bundle-cicd.yml
```

## Pipeline Layers

| Layer | Tables | What it does |
|-------|--------|-------------|
| **Bronze** | `bronze_customers`, `bronze_products`, `bronze_orders`, `bronze_order_items` | Raw CSV ingestion + `ingestion_timestamp` |
| **Silver** | `silver_customers`, `silver_products`, `silver_orders`, `silver_order_items`, `silver_quarantine` | Dedup, type casting, date parsing, null handling, referential integrity |
| **Gold** | `gold_daily_sales`, `gold_product_sales`, `gold_customer_sales` | Business aggregations (revenue, orders, items) |

## Environments

| Target | Schema | Mode | Tables Example |
|--------|--------|------|---------------|
| dev | `workspace.ecommerce_dev` | development | `workspace.ecommerce_dev.gold_daily_sales` |
| test | `workspace.ecommerce_test` | development | `workspace.ecommerce_test.gold_daily_sales` |
| prod | `workspace.ecommerce_prod` | production | `workspace.ecommerce_prod.gold_daily_sales` |

## Quick Start

### 1. Validate the bundle
```bash
databricks bundle validate -t dev
```

### 2. Deploy + upload data + run (dev)
```bash
databricks bundle deploy -t dev

# Upload CSVs to Volume
for file in data/*.csv; do
  databricks fs cp "$file" "dbfs:/Volumes/workspace/ecommerce_dev/raw_data/$(basename $file)" --overwrite
done

databricks bundle run -t dev ecommerce_pipeline
```

### 3. CI/CD (automatic)
Push to `main` → GitHub Actions runs: Validate → Test → Prod

## GitHub Secrets Required

| Secret | Description |
|--------|-------------|
| `DATABRICKS_HOST` | e.g., `https://dbc-4cc5c7f9-4515.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal Access Token from Databricks workspace |

## Tech Stack

- **PySpark** — data processing
- **Delta Lake** — table storage format
- **Unity Catalog** — data governance (workspace catalog)
- **DAB** — infrastructure-as-code for Databricks
- **GitHub Actions** — CI/CD automation
