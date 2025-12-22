-- Staging model for H&M customers
-- Cleans column names, handles nulls, improves readability

with source as (
    select * from {{ source('raw', 'customers') }}
),

cleaned as (
    select
        -- Primary key
        customer_id,
        
        -- Customer flags (convert to boolean-like)
        "FN" as has_first_name,
        "Active" as is_active,
        
        -- Membership
        club_member_status,
        fashion_news_frequency,
        
        -- Demographics
        age,
        postal_code,
        
        -- Age groups for analysis
        case
            when age is null then 'Unknown'
            when age < 25 then '18-24'
            when age < 35 then '25-34'
            when age < 45 then '35-44'
            when age < 55 then '45-54'
            when age < 65 then '55-64'
            else '65+'
        end as age_group

    from source
)

select * from cleaned