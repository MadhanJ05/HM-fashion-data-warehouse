-- H&M Fashion Data Warehouse Tables
-- Creates raw layer tables for H&M dataset

\c ecommerce

-- =============================================
-- CUSTOMERS TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id VARCHAR(64) PRIMARY KEY,
    fn FLOAT,
    active FLOAT,
    club_member_status VARCHAR(20),
    fashion_news_frequency VARCHAR(20),
    age FLOAT,
    postal_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- ARTICLES (PRODUCTS) TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS raw.articles (
    article_id INTEGER PRIMARY KEY,
    product_code INTEGER,
    prod_name VARCHAR(255),
    product_type_no INTEGER,
    product_type_name VARCHAR(100),
    product_group_name VARCHAR(100),
    graphical_appearance_no INTEGER,
    graphical_appearance_name VARCHAR(50),
    colour_group_code INTEGER,
    colour_group_name VARCHAR(50),
    perceived_colour_value_id INTEGER,
    perceived_colour_value_name VARCHAR(50),
    perceived_colour_master_id INTEGER,
    perceived_colour_master_name VARCHAR(50),
    department_no INTEGER,
    department_name VARCHAR(100),
    index_code VARCHAR(10),
    index_name VARCHAR(50),
    index_group_no INTEGER,
    index_group_name VARCHAR(50),
    section_no INTEGER,
    section_name VARCHAR(100),
    garment_group_no INTEGER,
    garment_group_name VARCHAR(50),
    detail_desc TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- TRANSACTIONS TABLE
-- =============================================
CREATE TABLE IF NOT EXISTS raw.transactions (
    id SERIAL,
    t_dat DATE NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    article_id INTEGER NOT NULL,
    price DECIMAL(10,4) NOT NULL,
    sales_channel_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_transactions_date ON raw.transactions(t_dat);
CREATE INDEX IF NOT EXISTS idx_transactions_customer ON raw.transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_article ON raw.transactions(article_id);

-- =============================================
-- GRANT PERMISSIONS
-- =============================================
GRANT ALL PRIVILEGES ON TABLE raw.customers TO airflow;
GRANT ALL PRIVILEGES ON TABLE raw.articles TO airflow;
GRANT ALL PRIVILEGES ON TABLE raw.transactions TO airflow;
GRANT USAGE, SELECT ON SEQUENCE raw.transactions_id_seq TO airflow;

-- Log success
DO $$
BEGIN
    RAISE NOTICE '✅ H&M tables created successfully!';
END $$;
