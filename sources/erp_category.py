import pygrametl
from pygrametl.datasources import SQLSource
from pygrametl.tables import Dimension


def extract_erp_categories(conn):
    """Extract ERP product categories from bronze layer"""
    query = """
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2
    """
    return SQLSource(connection=conn, query=query)


def load_erp_categories(conn_wrapper, source_conn):
    """Load ERP product categories into silver layer"""
    print("  Extracting ERP product categories from bronze...")
    source = extract_erp_categories(source_conn)
    
    # Define target table - simple passthrough, no transformations needed
    erp_cat_table = Dimension(
        name='erp_px_cat_g1v2',
        key='id',
        attributes=['cat', 'subcat', 'maintenance'],
        lookupatts=['id'],
        targetconnection=conn_wrapper
    )
    
    count = 0
    print("  Loading ERP product categories...")
    for row in source:
        row = dict(row)
        erp_cat_table.insert(row)
        count += 1
    
    conn_wrapper.commit()
    print(f"  ✓ Loaded {count} records into silver.erp_px_cat_g1v2")
    return count
