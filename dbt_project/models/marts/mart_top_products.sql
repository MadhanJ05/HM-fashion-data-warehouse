-- Mart: Top Products Analysis
-- Product performance metrics for dashboard

with product_data as (
    select * from {{ ref('int_product_sales') }}
),

ranked_products as (
    select
        article_id,
        product_name,
        product_type_name,
        product_group_name,
        colour_group_name,
        department_name,
        section_name,
        garment_group_name,
        
        -- Sales metrics
        total_sold,
        total_revenue,
        unique_customers,
        avg_price,
        
        -- Time metrics
        first_sale_date,
        last_sale_date,
        days_with_sales,
        
        -- Channel & season
        top_channel,
        peak_season,
        online_sales,
        store_sales,
        
        -- Rankings
        rank() over (order by total_sold desc) as rank_by_quantity,
        rank() over (order by total_revenue desc) as rank_by_revenue,
        rank() over (order by unique_customers desc) as rank_by_customers,
        
        -- Rankings within department
        rank() over (partition by department_name order by total_sold desc) as dept_rank_by_quantity

    from product_data
)

select
    *,
    
    -- Product tier based on sales
    case
        when rank_by_quantity <= 100 then 'Top 100'
        when rank_by_quantity <= 1000 then 'Top 1000'
        when rank_by_quantity <= 10000 then 'Top 10000'
        else 'Long Tail'
    end as product_tier,
    
    -- Is bestseller in department?
    case when dept_rank_by_quantity <= 10 then true else false end as is_dept_bestseller

from ranked_products