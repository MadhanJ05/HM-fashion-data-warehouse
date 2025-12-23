-- Mart: Customer RFM Segmentation
-- Recency, Frequency, Monetary analysis for customer segmentation

with customer_data as (
    select * from {{ ref('int_customer_orders') }}
),

-- Get the max date in dataset as reference point
date_reference as (
    select max(last_purchase_date) as analysis_date
    from customer_data
),

rfm_calc as (
    select
        c.customer_id,
        c.total_orders,
        c.total_spend,
        c.unique_products_purchased,
        c.first_purchase_date,
        c.last_purchase_date,
        c.preferred_channel,
        c.age_group,
        c.club_member_status,
        
        -- Recency: days since last purchase
        d.analysis_date - c.last_purchase_date as days_since_purchase,
        
        -- RFM Scores (1-5 scale using quintiles)
        ntile(5) over (order by c.last_purchase_date) as recency_score,
        ntile(5) over (order by c.total_orders) as frequency_score,
        ntile(5) over (order by c.total_spend) as monetary_score
        
    from customer_data c
    cross join date_reference d
),

rfm_segments as (
    select
        *,
        
        -- Combined RFM score
        recency_score + frequency_score + monetary_score as rfm_total,
        
        -- Customer segment based on RFM
        case
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4 then 'Champions'
            when recency_score >= 4 and frequency_score >= 3 then 'Loyal Customers'
            when recency_score >= 4 and frequency_score <= 2 then 'New Customers'
            when recency_score >= 3 and frequency_score >= 3 then 'Potential Loyalists'
            when recency_score >= 3 and frequency_score <= 2 then 'Promising'
            when recency_score <= 2 and frequency_score >= 4 then 'At Risk'
            when recency_score <= 2 and frequency_score >= 2 then 'Needs Attention'
            when recency_score <= 2 and frequency_score <= 2 and monetary_score >= 3 then 'About to Sleep'
            else 'Hibernating'
        end as customer_segment,
        
        -- Simple tier
        case
            when recency_score + frequency_score + monetary_score >= 12 then 'Gold'
            when recency_score + frequency_score + monetary_score >= 8 then 'Silver'
            else 'Bronze'
        end as customer_tier

    from rfm_calc
)

select * from rfm_segments