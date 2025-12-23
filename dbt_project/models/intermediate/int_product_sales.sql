-- Intermediate model: Product sales metrics
-- Aggregates transaction data per product

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

articles as (
    select * from {{ ref('stg_articles') }}
),

product_metrics as (
    select
        article_id,
        
        -- Sales counts
        count(*) as total_sold,
        count(distinct customer_id) as unique_customers,
        
        -- Revenue metrics
        sum(price) as total_revenue,
        avg(price) as avg_price,
        
        -- Time-based metrics
        min(transaction_date) as first_sale_date,
        max(transaction_date) as last_sale_date,
        count(distinct transaction_date) as days_with_sales,
        
        -- Channel breakdown
        sum(case when sales_channel = 'Online' then 1 else 0 end) as online_sales,
        sum(case when sales_channel = 'Store' then 1 else 0 end) as store_sales,
        
        -- Seasonal breakdown
        sum(case when season = 'Spring' then 1 else 0 end) as spring_sales,
        sum(case when season = 'Summer' then 1 else 0 end) as summer_sales,
        sum(case when season = 'Fall' then 1 else 0 end) as fall_sales,
        sum(case when season = 'Winter' then 1 else 0 end) as winter_sales

    from transactions
    group by article_id
)

select
    pm.*,
    a.product_name,
    a.product_type_name,
    a.product_group_name,
    a.colour_group_name,
    a.department_name,
    a.section_name,
    a.garment_group_name,
    
    -- Derived metrics
    case
        when pm.online_sales > pm.store_sales then 'Online'
        when pm.store_sales > pm.online_sales then 'Store'
        else 'Mixed'
    end as top_channel,
    
    -- Peak season
    case
        when greatest(pm.spring_sales, pm.summer_sales, pm.fall_sales, pm.winter_sales) = pm.spring_sales then 'Spring'
        when greatest(pm.spring_sales, pm.summer_sales, pm.fall_sales, pm.winter_sales) = pm.summer_sales then 'Summer'
        when greatest(pm.spring_sales, pm.summer_sales, pm.fall_sales, pm.winter_sales) = pm.fall_sales then 'Fall'
        else 'Winter'
    end as peak_season

from product_metrics pm
left join articles a on pm.article_id = a.article_id