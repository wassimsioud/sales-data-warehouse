import pygrametl
from pygrametl.datasources import SQLSource
from pygrametl.tables import FactTable
from datetime import datetime


def parse_date_int(value):
    """Parse integer date (YYYYMMDD) to date object"""
    if value is None or value == 0:
        return None
    str_val = str(value)
    if len(str_val) != 8:
        return None
    try:
        return datetime.strptime(str_val, '%Y%m%d').date()
    except ValueError:
        return None


def calculate_sales(row):
    """Calculate correct sales amount"""
    sls_sales = row.get('sls_sales')
    sls_quantity = row.get('sls_quantity') or 0
    sls_price = row.get('sls_price') or 0
    
    expected_sales = sls_quantity * abs(sls_price)
    
    if sls_sales is None or sls_sales <= 0 or sls_sales != expected_sales:
        return expected_sales
    return sls_sales


def calculate_price(row):
    """Calculate correct price"""
    sls_price = row.get('sls_price')
    sls_sales = row.get('sls_sales') or 0
    sls_quantity = row.get('sls_quantity') or 0
    
    # Derive price if original value is invalid
    if sls_price is None or sls_price <= 0:
        if sls_quantity != 0:
            return sls_sales / sls_quantity
        return 0
    return sls_price


def extract_sales(conn):
    """Extract sales from bronze layer"""
    query = """
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        FROM bronze.crm_sales_details
    """
    return SQLSource(connection=conn, query=query)


def transform_sales_row(row):
    """Apply transformations to a single sales row"""
    # Convert integer dates to proper dates
    row['sls_order_dt'] = parse_date_int(row['sls_order_dt'])
    row['sls_ship_dt'] = parse_date_int(row['sls_ship_dt'])
    row['sls_due_dt'] = parse_date_int(row['sls_due_dt'])
    
    # Calculate corrected sales and price
    row['sls_sales'] = calculate_sales(row)
    row['sls_price'] = calculate_price(row)
    
    return row


def load_sales(conn_wrapper, source_conn):
    """Load sales into silver layer"""
    print("  Extracting sales from bronze...")
    source = extract_sales(source_conn)
    
    # Define target table
    sales_table = FactTable(
        name='crm_sales_details',
        keyrefs=['sls_cust_id'],
        measures=['sls_ord_num', 'sls_prd_key', 'sls_order_dt', 'sls_ship_dt',
                 'sls_due_dt', 'sls_sales', 'sls_quantity', 'sls_price'],
        targetconnection=conn_wrapper
    )
    
    count = 0
    print("  Transforming and loading sales...")
    for row in source:
        row = dict(row)  # Convert to mutable dict
        row = transform_sales_row(row)
        sales_table.insert(row)
        count += 1
    
    conn_wrapper.commit()
    print(f"  ✓ Loaded {count} sales records into silver.crm_sales_details")
    return count
