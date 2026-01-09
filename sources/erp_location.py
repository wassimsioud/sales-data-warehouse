import pygrametl
from pygrametl.datasources import SQLSource
from pygrametl.tables import Dimension


def clean_cid(value):
    """Remove dashes from customer ID"""
    if value is None:
        return None
    return str(value).replace('-', '')


def transform_country(value):
    """Normalize country codes to full names"""
    if value is None or str(value).strip() == '':
        return 'n/a'
    
    value = str(value).strip()
    
    # Country code mappings
    country_map = {
        'DE': 'Germany',
        'US': 'United States',
        'USA': 'United States'
    }
    
    return country_map.get(value, value)


def extract_erp_locations(conn):
    """Extract ERP locations from bronze layer"""
    query = """
        SELECT cid, cntry
        FROM bronze.erp_loc_a101
    """
    return SQLSource(connection=conn, query=query)


def transform_erp_location_row(row):
    """Apply transformations to ERP location row"""
    row['cid'] = clean_cid(row['cid'])
    row['cntry'] = transform_country(row['cntry'])
    return row


def load_erp_locations(conn_wrapper, source_conn):
    """Load ERP locations into silver layer"""
    print("  Extracting ERP locations from bronze...")
    source = extract_erp_locations(source_conn)
    
    # Define target table
    erp_loc_table = Dimension(
        name='erp_loc_a101',
        key='cid',
        attributes=['cntry'],
        lookupatts=['cid'],
        targetconnection=conn_wrapper
    )
    
    count = 0
    print("  Transforming and loading ERP locations...")
    for row in source:
        row = dict(row)
        row = transform_erp_location_row(row)
        erp_loc_table.insert(row)
        count += 1
    
    conn_wrapper.commit()
    print(f"  ✓ Loaded {count} records into silver.erp_loc_a101")
    return count
