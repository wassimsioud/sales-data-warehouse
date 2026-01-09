from pygrametl.datasources import SQLSource
from pygrametl.tables import Dimension


def extract_dim_products(conn):
    """
    Extract and join product data from Silver layer
    Combines: crm_prd_info + erp_px_cat_g1v2
    Filters out historical data (only current products where prd_end_dt IS NULL)
    """
    query = """
        SELECT
            pn.prd_id AS product_id,
            pn.prd_key AS product_number,
            pn.prd_nm AS product_name,
            pn.cat_id AS category_id,
            pc.cat AS category,
            pc.subcat AS subcategory,
            pc.maintenance AS maintenance,
            pn.prd_cost AS cost,
            pn.prd_line AS product_line,
            pn.prd_start_dt AS start_date
        FROM silver.crm_prd_info pn
        LEFT JOIN silver.erp_px_cat_g1v2 pc
            ON pn.cat_id = pc.id
        WHERE pn.prd_end_dt IS NULL
        ORDER BY pn.prd_start_dt, pn.prd_key
    """
    return SQLSource(connection=conn, query=query)


def load_dim_products(conn_wrapper, source_conn):
    """Load products dimension into Gold layer"""
    print("  Extracting product dimension from Silver...")
    source = extract_dim_products(source_conn)
    
    # Define the dimension table with surrogate key
    dim_products = Dimension(
        name='dim_products',
        key='product_key',
        attributes=['product_id', 'product_number', 'product_name', 'category_id',
                   'category', 'subcategory', 'maintenance', 'cost', 'product_line', 
                   'start_date'],
        lookupatts=['product_number'],
        targetconnection=conn_wrapper
    )
    
    count = 0
    print("  Loading dim_products...")
    for row in source:
        row = dict(row)
        # Handle NULL values
        row['category'] = row['category'] if row['category'] else 'n/a'
        row['subcategory'] = row['subcategory'] if row['subcategory'] else 'n/a'
        row['maintenance'] = row['maintenance'] if row['maintenance'] else 'n/a'
        row['cost'] = row['cost'] if row['cost'] is not None else 0
        
        dim_products.insert(row)
        count += 1
    
    conn_wrapper.commit()
    print(f"  ✓ Loaded {count} rows into gold.dim_products")
    return count


def get_product_key_lookup(conn):
    """Get product_number to product_key mapping for fact table loading"""
    cur = conn.cursor()
    cur.execute("SELECT product_number, product_key FROM gold.dim_products")
    lookup = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    return lookup
