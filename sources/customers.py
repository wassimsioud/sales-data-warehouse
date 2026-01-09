import pygrametl
from pygrametl.datasources import SQLSource
from pygrametl.tables import Dimension, FactTable
from datetime import datetime


def transform_marital_status(value):
    """Transform marital status codes to descriptive values"""
    if value is None:
        return 'n/a'
    value = str(value).strip().upper()
    if value == 'M':
        return 'Married'
    elif value == 'S':
        return 'Single'
    return 'n/a'


def transform_gender(value):
    """Transform gender codes to descriptive values"""
    if value is None:
        return 'n/a'
    value = str(value).strip().upper()
    if value == 'F':
        return 'Female'
    elif value == 'M':
        return 'Male'
    return 'n/a'


def clean_name(value):
    """Trim and clean name fields"""
    if value is None:
        return None
    return str(value).strip()


def extract_customers(conn):
    """Extract customers from bronze layer with deduplication"""
    query = """
        SELECT 
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1
    """
    return SQLSource(connection=conn, query=query)


def transform_customer_row(row):
    """Apply transformations to a single customer row"""
    row['cst_firstname'] = clean_name(row['cst_firstname'])
    row['cst_lastname'] = clean_name(row['cst_lastname'])
    row['cst_marital_status'] = transform_marital_status(row['cst_marital_status'])
    row['cst_gndr'] = transform_gender(row['cst_gndr'])
    return row


def load_customers(conn_wrapper, source_conn):
    print("  Extracting customers from bronze...")
    source = extract_customers(source_conn)
    
    customer_table = Dimension(
        name='crm_cust_info',
        key='cst_id',
        attributes=['cst_key', 'cst_firstname', 'cst_lastname', 
                   'cst_marital_status', 'cst_gndr', 'cst_create_date'],
        lookupatts=['cst_id'],
        targetconnection=conn_wrapper
    )
    
    count = 0
    print("  Transforming and loading customers...")
    for row in source:
        row = transform_customer_row(row)
        customer_table.insert(row)
        count += 1
    
    conn_wrapper.commit()
    print(f"  ✓ Loaded {count} customers into silver.crm_cust_info")
    return count
