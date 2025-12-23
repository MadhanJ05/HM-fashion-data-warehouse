"""
H&M Fashion Analytics Dashboard
================================
Interactive dashboard for H&M fashion data warehouse insights.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os

# =============================================
# DATABASE CONNECTION
# =============================================

@st.cache_resource
def get_connection():
    """Create database connection."""
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'ecommerce')
    DB_USER = os.getenv('DB_USER', 'airflow')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
    
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

@st.cache_data(ttl=600)
def run_query(query):
    """Run SQL query and return dataframe."""
    engine = get_connection()
    return pd.read_sql(query, engine)


# =============================================
# PAGE CONFIG
# =============================================

st.set_page_config(
    page_title="H&M Fashion Analytics",
    page_icon="👗",
    layout="wide"
)

st.title("👗 H&M Fashion Analytics Dashboard")
st.markdown("---")


# =============================================
# KEY METRICS
# =============================================

st.header("📊 Key Metrics")

col1, col2, col3, col4 = st.columns(4)

# Total customers
customers_count = run_query("SELECT COUNT(*) as count FROM raw_marts.mart_customer_rfm")
col1.metric("Total Customers", f"{customers_count['count'].iloc[0]:,}")

# Total transactions
transactions_count = run_query("SELECT COUNT(*) as count FROM raw.transactions")
col2.metric("Total Transactions", f"{transactions_count['count'].iloc[0]:,}")

# Total products
products_count = run_query("SELECT COUNT(*) as count FROM raw.articles")
col3.metric("Total Products", f"{products_count['count'].iloc[0]:,}")

# Champions count
champions_count = run_query("SELECT COUNT(*) as count FROM raw_marts.mart_customer_rfm WHERE customer_segment = 'Champions'")
col4.metric("Champion Customers", f"{champions_count['count'].iloc[0]:,}")

st.markdown("---")


# =============================================
# CUSTOMER SEGMENTATION
# =============================================

st.header("👥 Customer Segmentation")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Customer Segments")
    segment_data = run_query("""
        SELECT customer_segment, COUNT(*) as count 
        FROM raw_marts.mart_customer_rfm 
        GROUP BY customer_segment 
        ORDER BY count DESC
    """)
    
    fig_segment = px.pie(
        segment_data, 
        values='count', 
        names='customer_segment',
        title='Distribution by RFM Segment',
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.plotly_chart(fig_segment, use_container_width=True)

with col2:
    st.subheader("Customer Tiers")
    tier_data = run_query("""
        SELECT customer_tier, COUNT(*) as count 
        FROM raw_marts.mart_customer_rfm 
        GROUP BY customer_tier 
        ORDER BY count DESC
    """)
    
    fig_tier = px.bar(
        tier_data,
        x='customer_tier',
        y='count',
        title='Distribution by Customer Tier',
        color='customer_tier',
        color_discrete_map={'Gold': '#FFD700', 'Silver': '#C0C0C0', 'Bronze': '#CD7F32'}
    )
    st.plotly_chart(fig_tier, use_container_width=True)

st.markdown("---")


# =============================================
# MONTHLY SALES TRENDS
# =============================================

st.header("📈 Monthly Sales Trends")

monthly_data = run_query("""
    SELECT 
        month_date,
        month_name,
        total_transactions,
        unique_customers,
        total_revenue,
        online_percentage,
        revenue_growth_pct
    FROM raw_marts.mart_monthly_sales
    ORDER BY month_date
""")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Monthly Transactions")
    fig_transactions = px.line(
        monthly_data,
        x='month_date',
        y='total_transactions',
        title='Transactions Over Time',
        markers=True
    )
    fig_transactions.update_layout(xaxis_title='Month', yaxis_title='Transactions')
    st.plotly_chart(fig_transactions, use_container_width=True)

with col2:
    st.subheader("Monthly Revenue")
    fig_revenue = px.line(
        monthly_data,
        x='month_date',
        y='total_revenue',
        title='Revenue Over Time',
        markers=True,
        color_discrete_sequence=['#00CC96']
    )
    fig_revenue.update_layout(xaxis_title='Month', yaxis_title='Revenue')
    st.plotly_chart(fig_revenue, use_container_width=True)

# Online vs Store
st.subheader("Online vs Store Sales")
fig_channel = px.bar(
    monthly_data,
    x='month_date',
    y='online_percentage',
    title='Online Sales Percentage by Month',
    color_discrete_sequence=['#636EFA']
)
fig_channel.update_layout(xaxis_title='Month', yaxis_title='Online %')
st.plotly_chart(fig_channel, use_container_width=True)

st.markdown("---")


# =============================================
# TOP PRODUCTS
# =============================================

st.header("🏆 Top Products")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 10 by Sales Volume")
    top_products = run_query("""
        SELECT product_name, total_sold, department_name
        FROM raw_marts.mart_top_products
        ORDER BY total_sold DESC
        LIMIT 10
    """)
    
    fig_top = px.bar(
        top_products,
        x='total_sold',
        y='product_name',
        orientation='h',
        title='Best Selling Products',
        color='department_name'
    )
    fig_top.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig_top, use_container_width=True)

with col2:
    st.subheader("Sales by Department")
    dept_data = run_query("""
        SELECT department_name, SUM(total_sold) as total_sales
        FROM raw_marts.mart_top_products
        GROUP BY department_name
        ORDER BY total_sales DESC
        LIMIT 10
    """)
    
    fig_dept = px.pie(
        dept_data,
        values='total_sales',
        names='department_name',
        title='Sales Distribution by Department'
    )
    st.plotly_chart(fig_dept, use_container_width=True)

st.markdown("---")


# =============================================
# AGE GROUP ANALYSIS
# =============================================

st.header("📊 Customer Demographics")

age_data = run_query("""
    SELECT age_group, customer_tier, COUNT(*) as count
    FROM raw_marts.mart_customer_rfm
    WHERE age_group != 'Unknown'
    GROUP BY age_group, customer_tier
    ORDER BY age_group
""")

fig_age = px.bar(
    age_data,
    x='age_group',
    y='count',
    color='customer_tier',
    title='Customer Tiers by Age Group',
    barmode='group',
    color_discrete_map={'Gold': '#FFD700', 'Silver': '#C0C0C0', 'Bronze': '#CD7F32'}
)
st.plotly_chart(fig_age, use_container_width=True)

st.markdown("---")


# =============================================
# FOOTER
# =============================================

st.markdown("""
### About This Dashboard
This dashboard visualizes data from the H&M Fashion Data Warehouse, processing **15M+ transactions** 
across **1M customers** and **105K products**.

**Tech Stack:** PostgreSQL | dbt | Streamlit | Plotly

**Data Source:** [H&M Personalized Fashion Recommendations](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations) - Kaggle
""")