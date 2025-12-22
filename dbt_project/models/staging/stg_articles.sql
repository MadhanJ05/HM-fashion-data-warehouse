-- Staging model for H&M articles (products)
-- Cleans column names and selects relevant fields

with source as (
    select * from {{ source('raw', 'articles') }}
),

cleaned as (
    select
        -- Primary key
        article_id,
        product_code,
        
        -- Product info
        prod_name as product_name,
        product_type_name,
        product_group_name,
        
        -- Appearance
        graphical_appearance_name,
        colour_group_name,
        perceived_colour_value_name as colour_value,
        perceived_colour_master_name as colour_master,
        
        -- Department & category
        department_name,
        index_name,
        index_group_name,
        section_name,
        garment_group_name,
        
        -- Description
        detail_desc as description

    from source
)

select * from cleaned