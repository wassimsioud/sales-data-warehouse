from pygrametl.datasources import SQLSource
from pygrametl.tables import FactTable

from dimensions.dim_customers import get_customer_key_lookup
from dimensions.dim_products import get_product_key_lookup


def extract_fact_sales(conn):
    """
    Extract sales data from Silver layer
    """
    query = """
        SELECT
            sls_ord_num AS order_number,
            sls_prd_key AS product_number,
            sls_cust_id AS customer_id,
            sls_order_dt AS order_date,
            sls_ship_dt AS shipping_date,
            sls_due_dt AS due_date,
            sls_sales AS sales_amount,
            sls_quantity AS quantity,
            sls_price AS price
        FROM silver.crm_sales_details
    """
    return SQLSource(connection=conn, query=query)


def load_fact_sales(conn_wrapper, source_conn, target_conn):
    """Load sales fact table into Gold layer with dimension key lookups"""
    print("  Building dimension key lookups...")
    
    # Get dimension key mappings
    customer_lookup = get_customer_key_lookup(target_conn)
    product_lookup = get_product_key_lookup(target_conn)
    
    print(f"    → Customer keys: {len(customer_lookup)}")
    print(f"    → Product keys: {len(product_lookup)}")
    
    print("  Extracting sales facts from Silver...")
    source = extract_fact_sales(source_conn)
    
    # Define the fact table
    fact_sales = FactTable(
        name='fact_sales',
        keyrefs=['product_key', 'customer_key'],
        measures=['order_number', 'order_date', 'shipping_date', 'due_date',
                 'sales_amount', 'quantity', 'price'],
        targetconnection=conn_wrapper
    )
    
    count = 0
    skipped = 0
    missing_customers = set()
    missing_products = set()
    
    print("  Loading fact_sales...")
    for row in source:
        row = dict(row)
        
        # Lookup surrogate keys
        customer_id = row.pop('customer_id')
        product_number = row.pop('product_number')
        
        customer_key = customer_lookup.get(customer_id)
        product_key = product_lookup.get(product_number)
        
        # Track missing dimension members
        if customer_key is None:
            missing_customers.add(customer_id)
            skipped += 1
            continue
            
        if product_key is None:
            missing_products.add(product_number)
            skipped += 1
            continue
        
        # Add surrogate keys to row
        row['customer_key'] = customer_key
        row['product_key'] = product_key
        
        fact_sales.insert(row)
        count += 1
        
        if count % 10000 == 0:
            print(f"    Loaded {count:,} sales records...")
    
    conn_wrapper.commit()
    
    print(f"  ✓ Loaded {count} rows into gold.fact_sales")
    
    if skipped > 0:
        print(f"  ⚠ Skipped {skipped} rows due to missing dimension keys")
        if missing_customers:
            print(f"    → {len(missing_customers)} unknown customer IDs")
        if missing_products:
            print(f"    → {len(missing_products)} unknown product numbers")
    
    return count
