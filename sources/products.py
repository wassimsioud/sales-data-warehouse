import pygrametl
from pygrametl.datasources import SQLSource
from pygrametl.tables import Dimension


def transform_product_line(value):
    """Transform product line codes to descriptive values"""
    if value is None:
        return 'n/a'
    value = str(value).strip().upper()
    mapping = {
        'M': 'Mountain',
        'R': 'Road',
        'S': 'Other Sales',
        'T': 'Touring'
    }
    return mapping.get(value, 'n/a')


def extract_cat_id(prd_key):
    """Extract category ID from product key"""
    if prd_key is None:
        return None
    # Get first 5 chars and replace - with _
    return prd_key[:5].replace('-', '_') if len(prd_key) >= 5 else prd_key


def extract_prd_key(prd_key):
    """Extract product key portion from full key"""
    if prd_key is None:
        return None
    # Get from position 7 onwards (0-indexed: 6)
    return prd_key[6:] if len(prd_key) > 6 else prd_key


def extract_products(conn):
    """Extract products from bronze layer with end date calculation"""
    query = """
        SELECT 
            prd_id,
            prd_key AS original_prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            CAST(
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
                AS DATE
            ) AS prd_end_dt
        FROM bronze.crm_prd_info
    """
    return SQLSource(connection=conn, query=query)


def transform_product_row(row):
    """Apply transformations to a single product row"""
    # Extract cat_id and prd_key from original key
    original_key = row.get('original_prd_key', '')
    row['cat_id'] = extract_cat_id(original_key)
    row['prd_key'] = extract_prd_key(original_key)
    
    # Clean cost - default to 0 if null
    row['prd_cost'] = row['prd_cost'] if row['prd_cost'] is not None else 0
    
    # Transform product line
    row['prd_line'] = transform_product_line(row['prd_line'])
    
    # Remove the original key field
    if 'original_prd_key' in row:
        del row['original_prd_key']
    
    return row


def load_products(conn_wrapper, source_conn):
    """Load products into silver layer"""
    print("  Extracting products from bronze...")
    source = extract_products(source_conn)
    
    # Define target table
    product_table = Dimension(
        name='crm_prd_info',
        key='prd_id',
        attributes=['cat_id', 'prd_key', 'prd_nm', 'prd_cost', 
                   'prd_line', 'prd_start_dt', 'prd_end_dt'],
        lookupatts=['prd_id'],
        targetconnection=conn_wrapper
    )
    
    count = 0
    print("  Transforming and loading products...")
    for row in source:
        row = dict(row)  # Convert to mutable dict
        row = transform_product_row(row)
        product_table.insert(row)
        count += 1
    
    conn_wrapper.commit()
    print(f"  ✓ Loaded {count} products into silver.crm_prd_info")
    return count
