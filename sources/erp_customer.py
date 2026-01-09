import pygrametl
from pygrametl.datasources import SQLSource
from pygrametl.tables import Dimension
from datetime import date


def clean_cid(value):
    """Remove 'NAS' prefix from customer ID"""
    if value is None:
        return None
    value = str(value)
    if value.startswith('NAS'):
        return value[3:]  
    return value


def validate_birthdate(bdate):
    """Set future birthdates to NULL"""
    if bdate is None:
        return None
    if bdate > date.today():
        return None
    return bdate


def transform_gender(value):
    """Normalize gender values"""
    if value is None:
        return 'n/a'
    value = str(value).strip().upper()
    if value in ('F', 'FEMALE'):
        return 'Female'
    elif value in ('M', 'MALE'):
        return 'Male'
    return 'n/a'


def extract_erp_customers(conn):
    """Extract ERP customer demographics from bronze layer"""
    query = """
        SELECT cid, bdate, gen
        FROM bronze.erp_cust_az12
    """
    return SQLSource(connection=conn, query=query)


def transform_erp_customer_row(row):
    """Apply transformations to ERP customer row"""
    row['cid'] = clean_cid(row['cid'])
    row['bdate'] = validate_birthdate(row['bdate'])
    row['gen'] = transform_gender(row['gen'])
    return row


def load_erp_customers(conn_wrapper, source_conn):
    """Load ERP customer demographics into silver layer"""
    print("  Extracting ERP customer demographics from bronze...")
    source = extract_erp_customers(source_conn)
    
    # Define target table
    erp_cust_table = Dimension(
        name='erp_cust_az12',
        key='cid',
        attributes=['bdate', 'gen'],
        lookupatts=['cid'],
        targetconnection=conn_wrapper
    )
    
    count = 0
    print("  Transforming and loading ERP customer demographics...")
    for row in source:
        row = dict(row)
        row = transform_erp_customer_row(row)
        erp_cust_table.insert(row)
        count += 1
    
    conn_wrapper.commit()
    print(f"  ✓ Loaded {count} records into silver.erp_cust_az12")
    return count
