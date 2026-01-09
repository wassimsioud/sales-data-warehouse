"""
Database connection configuration for the Data Warehouse ETL
"""
import psycopg2

DB_CONFIG = {
    'dbname': 'datawarehouse',
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': '12345678'
}


def get_connection():
    """Create and return a database connection"""
    conn = psycopg2.connect(**DB_CONFIG)
    print('Connection successful')
    return conn


def create_silver_tables(conn):
    """Create silver layer tables"""
    cur = conn.cursor()
    cur.execute("SET search_path = 'silver'")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.crm_cust_info (
            cst_id INT,
            cst_key TEXT,
            cst_firstname TEXT,
            cst_lastname TEXT,
            cst_marital_status TEXT,
            cst_gndr TEXT,
            cst_create_date DATE,
            dwh_create_date TIMESTAMPTZ DEFAULT now()
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.crm_prd_info (
            prd_id INTEGER,
            cat_id TEXT,
            prd_key TEXT,
            prd_nm TEXT,
            prd_cost INTEGER,
            prd_line TEXT,
            prd_start_dt DATE,
            prd_end_dt DATE,
            dwh_create_date TIMESTAMPTZ DEFAULT now()
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.crm_sales_details (
            sls_ord_num TEXT,
            sls_prd_key TEXT,
            sls_cust_id INTEGER,
            sls_order_dt DATE,
            sls_ship_dt DATE,
            sls_due_dt DATE,
            sls_sales INTEGER,
            sls_quantity INTEGER,
            sls_price INTEGER,
            dwh_create_date TIMESTAMPTZ DEFAULT now()
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.erp_loc_a101 (
            cid TEXT,
            cntry TEXT,
            dwh_create_date TIMESTAMPTZ DEFAULT now()
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.erp_cust_az12 (
            cid TEXT,
            bdate DATE,
            gen TEXT,
            dwh_create_date TIMESTAMPTZ DEFAULT now()
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2 (
            id TEXT,
            cat TEXT,
            subcat TEXT,
            maintenance TEXT,
            dwh_create_date TIMESTAMPTZ DEFAULT now()
        );
    """)
    
    conn.commit()
    cur.close()
    print("Silver tables created successfully")


def truncate_silver_tables(conn):
    """Truncate all silver tables before reload"""
    cur = conn.cursor()
    tables = [
        'silver.crm_cust_info',
        'silver.crm_prd_info', 
        'silver.crm_sales_details',
        'silver.erp_loc_a101',
        'silver.erp_cust_az12',
        'silver.erp_px_cat_g1v2'
    ]
    for table in tables:
        cur.execute(f"TRUNCATE TABLE {table};")
    conn.commit()
    cur.close()
    print("Silver tables truncated")


def create_gold_tables(conn):
    """Create gold layer tables (Star Schema)"""
    cur = conn.cursor()
    
    cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold.dim_customers (
            customer_key SERIAL PRIMARY KEY,
            customer_id INTEGER,
            customer_number TEXT,
            first_name TEXT,
            last_name TEXT,
            country TEXT,
            marital_status TEXT,
            gender TEXT,
            birthdate DATE,
            create_date DATE,
            dwh_create_date TIMESTAMPTZ DEFAULT now()
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold.dim_products (
            product_key SERIAL PRIMARY KEY,
            product_id INTEGER,
            product_number TEXT,
            product_name TEXT,
            category_id TEXT,
            category TEXT,
            subcategory TEXT,
            maintenance TEXT,
            cost INTEGER,
            product_line TEXT,
            start_date DATE,
            dwh_create_date TIMESTAMPTZ DEFAULT now()
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold.fact_sales (
            sale_key SERIAL PRIMARY KEY,
            order_number TEXT,
            product_key INTEGER REFERENCES gold.dim_products(product_key),
            customer_key INTEGER REFERENCES gold.dim_customers(customer_key),
            order_date DATE,
            shipping_date DATE,
            due_date DATE,
            sales_amount INTEGER,
            quantity INTEGER,
            price INTEGER,
            dwh_create_date TIMESTAMPTZ DEFAULT now()
        );
    """)
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON gold.fact_sales(customer_key);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON gold.fact_sales(product_key);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date ON gold.fact_sales(order_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_customers_number ON gold.dim_customers(customer_number);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dim_products_number ON gold.dim_products(product_number);")
    
    conn.commit()
    cur.close()
    print("Gold tables (Star Schema) created successfully")


def truncate_gold_tables(conn):
    """Truncate all gold tables before reload"""
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE gold.fact_sales CASCADE;")
    cur.execute("TRUNCATE TABLE gold.dim_customers CASCADE;")
    cur.execute("TRUNCATE TABLE gold.dim_products CASCADE;")
    conn.commit()
    cur.close()
    print("Gold tables truncated")
