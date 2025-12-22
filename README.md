# H&M Fashion Data Warehouse

Production-grade data warehouse processing **15M+ fashion retail transactions** from H&M's personalized recommendations dataset using Apache Airflow, dbt, PostgreSQL, and Streamlit.

---

## 📊 Project Overview

This project demonstrates end-to-end data engineering by building a complete analytics pipeline for fashion retail data. It processes one year of H&M transaction data (Sept 2019 - Sept 2020) covering 1 million customers and 105K+ products.

### Key Metrics

| Metric | Value |
|--------|-------|
| Transactions | 15M+ |
| Customers | ~1M |
| Products | 105K+ |
| Date Range | 12 months |

---

## 🏗️ Architecture
```
H&M Dataset (Kaggle)
        ↓
   Airflow DAGs (Orchestration)
        ↓
   PostgreSQL (Raw Layer)
        ↓
   dbt Models (Staging → Intermediate → Marts)
        ↓
   Streamlit Dashboard (Visualization)
```

### Data Flow

1. **Ingestion**: Airflow loads H&M transaction data into PostgreSQL
2. **Staging**: dbt models clean and standardize raw data
3. **Modeling**: Dimensional models (star schema) for analytics
4. **Marts**: Business metrics — customer segmentation, product performance, seasonal trends
5. **Visualization**: Interactive Streamlit dashboard

---

## ✨ Features

### Data Engineering
- Automated ETL pipelines with Airflow
- Incremental data loading for 15M+ records
- Data quality testing (null checks, referential integrity)
- Dimensional modeling (fact & dimension tables)

### Analytics
- Customer segmentation (RFM analysis)
- Seasonal fashion trend analysis
- Product performance tracking
- Revenue and sales metrics

### Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow 2.7 |
| Transformation | dbt 1.7 |
| Database | PostgreSQL 15 |
| Visualization | Streamlit |
| Containerization | Docker |

---

## 🚀 Quick Start

### Prerequisites
- GitHub Codespaces (recommended) OR Docker Desktop
- 8GB RAM minimum

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/HM-fashion-data-warehouse.git
cd HM-fashion-data-warehouse
```

2. **Start services**
```bash
docker-compose up -d
```

3. **Access the tools**
- Airflow UI: http://localhost:8080
- Streamlit Dashboard: http://localhost:8501

---

## 📁 Project Structure
```
HM-fashion-data-warehouse/
├── airflow/
│   └── dags/
├── dbt_project/
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── data/
│   └── raw/
├── dashboard/
└── docker-compose.yml
```

---

## 📈 Sample Insights

- Seasonal purchasing patterns across spring, summer, fall, winter
- Customer lifetime value segmentation
- Top-performing product categories
- Repeat purchase behavior analysis

---

## 🎯 Skills Demonstrated

- **Data Engineering**: ETL pipelines, data modeling, orchestration
- **SQL**: Complex transformations, window functions, CTEs
- **Python**: Data processing, automation
- **DevOps**: Docker, version control
- **Analytics**: Business metrics, segmentation

---

## 📚 Data Source

[H&M Personalized Fashion Recommendations](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations) — Kaggle Competition Dataset

---

## 👤 Author

Madhan Jothimani — [LinkedIn](http://www.linkedin.com/in/madhanjothimani)) | [GitHub]([your-github](https://github.com/MadhanJ05?tab=repositories))
