-- Intermediate model: Customer order metrics
-- Aggregates transaction data per customer

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

customer_metrics as (
    select
        customer_id,
        
        -- Order counts
        count(*) as total_orders,
        count(distinct transaction_date) as unique_purchase_days,
        
        -- Revenue metrics
        sum(price) as total_spend,
        avg(price) as avg_order_value,
        min(price) as min_order_value,
        max(price) as max_order_value,
        
        -- Product diversity
        count(distinct article_id) as unique_products_purchased,
        
        -- Time-based metrics
        min(transaction_date) as first_purchase_date,
        max(transaction_date) as last_purchase_date,
        max(transaction_date) - min(transaction_date) as customer_lifespan_days,
        
        -- Channel preference
        sum(case when sales_channel = 'Online' then 1 else 0 end) as online_orders,
        sum(case when sales_channel = 'Store' then 1 else 0 end) as store_orders,
        
        -- Seasonal preference
        sum(case when season = 'Spring' then 1 else 0 end) as spring_orders,
        sum(case when season = 'Summer' then 1 else 0 end) as summer_orders,
        sum(case when season = 'Fall' then 1 else 0 end) as fall_orders,
        sum(case when season = 'Winter' then 1 else 0 end) as winter_orders

    from transactions
    group by customer_id
)

select
    cm.*,
    c.age,
    c.age_group,
    c.club_member_status,
    c.fashion_news_frequency,
    
    -- Derived metrics
    case
        when cm.online_orders > cm.store_orders then 'Online'
        when cm.store_orders > cm.online_orders then 'Store'
        else 'Mixed'
    end as preferred_channel,
    
    -- Favorite season
    case
        when greatest(cm.spring_orders, cm.summer_orders, cm.fall_orders, cm.winter_orders) = cm.spring_orders then 'Spring'
        when greatest(cm.spring_orders, cm.summer_orders, cm.fall_orders, cm.winter_orders) = cm.summer_orders then 'Summer'
        when greatest(cm.spring_orders, cm.summer_orders, cm.fall_orders, cm.winter_orders) = cm.fall_orders then 'Fall'
        else 'Winter'
    end as favorite_season

from customer_metrics cm
left join customers c on cm.customer_id = c.customer_id