-- Staging model for H&M transactions
-- Cleans column names, adds date parts for analysis

with source as (
    select * from {{ source('raw', 'transactions') }}
),

cleaned as (
    select
        -- Keys
        customer_id,
        article_id,
        
        -- Transaction details (cast text to date)
        t_dat::date as transaction_date,
        price,
        sales_channel_id,
        
        -- Sales channel name
        case sales_channel_id
            when 1 then 'Online'
            when 2 then 'Store'
            else 'Unknown'
        end as sales_channel,
        
        -- Date parts for analysis
        extract(year from t_dat::date) as transaction_year,
        extract(month from t_dat::date) as transaction_month,
        extract(dow from t_dat::date) as day_of_week,
        to_char(t_dat::date, 'Month') as month_name,
        to_char(t_dat::date, 'Day') as day_name,
        
        -- Fiscal quarters
        case
            when extract(month from t_dat::date) in (1, 2, 3) then 'Q1'
            when extract(month from t_dat::date) in (4, 5, 6) then 'Q2'
            when extract(month from t_dat::date) in (7, 8, 9) then 'Q3'
            else 'Q4'
        end as quarter,
        
        -- Season (fashion relevant)
        case
            when extract(month from t_dat::date) in (3, 4, 5) then 'Spring'
            when extract(month from t_dat::date) in (6, 7, 8) then 'Summer'
            when extract(month from t_dat::date) in (9, 10, 11) then 'Fall'
            else 'Winter'
        end as season

    from source
)

select * from cleaned