-- Mart: Monthly Sales Metrics
-- Aggregated sales data by month for trend analysis

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

monthly_metrics as (
    select
        transaction_year,
        transaction_month,
        month_name,
        quarter,
        season,
        
        -- Sales metrics
        count(*) as total_transactions,
        count(distinct customer_id) as unique_customers,
        count(distinct article_id) as unique_products_sold,
        sum(price) as total_revenue,
        avg(price) as avg_transaction_value,
        
        -- Channel breakdown
        sum(case when sales_channel = 'Online' then 1 else 0 end) as online_transactions,
        sum(case when sales_channel = 'Store' then 1 else 0 end) as store_transactions,
        sum(case when sales_channel = 'Online' then price else 0 end) as online_revenue,
        sum(case when sales_channel = 'Store' then price else 0 end) as store_revenue

    from transactions
    group by 
        transaction_year,
        transaction_month,
        month_name,
        quarter,
        season
)

select
    *,
    
    -- Create a sortable date column
    to_date(transaction_year || '-' || lpad(transaction_month::text, 2, '0') || '-01', 'YYYY-MM-DD') as month_date,
    
    -- Online percentage
    round((100.0 * online_transactions / nullif(total_transactions, 0))::numeric, 1) as online_percentage,
    
    -- Month-over-month growth
    lag(total_revenue) over (order by transaction_year, transaction_month) as prev_month_revenue,
    
    -- Revenue growth
    round((100.0 * (total_revenue - lag(total_revenue) over (order by transaction_year, transaction_month)) 
        / nullif(lag(total_revenue) over (order by transaction_year, transaction_month), 0))::numeric, 1) as revenue_growth_pct

from monthly_metrics
order by transaction_year, transaction_month