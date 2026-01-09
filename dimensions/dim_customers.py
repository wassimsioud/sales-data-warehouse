from pygrametl.datasources import SQLSource
from pygrametl.tables import Dimension


def extract_dim_customers(conn):
    """
    Extract and join customer data from Silver layer
    Combines: crm_cust_info + erp_cust_az12 + erp_loc_a101
    """
    query = """
        SELECT
            ci.cst_id AS customer_id,
            ci.cst_key AS customer_number,
            ci.cst_firstname AS first_name,
            ci.cst_lastname AS last_name,
            la.cntry AS country,
            ci.cst_marital_status AS marital_status,
            CASE 
                WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
                ELSE COALESCE(ca.gen, 'n/a')
            END AS gender,
            ca.bdate AS birthdate,
            ci.cst_create_date AS create_date
        FROM silver.crm_cust_info ci
        LEFT JOIN silver.erp_cust_az12 ca
            ON ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 la
            ON ci.cst_key = la.cid
        ORDER BY ci.cst_id
    """
    return SQLSource(connection=conn, query=query)


def load_dim_customers(conn_wrapper, source_conn):
    """Load customers dimension into Gold layer"""
    print("  Extracting customer dimension from Silver...")
    source = extract_dim_customers(source_conn)
    
    dim_customers = Dimension(
        name='dim_customers',
        key='customer_key',
        attributes=['customer_id', 'customer_number', 'first_name', 'last_name',
                   'country', 'marital_status', 'gender', 'birthdate', 'create_date'],
        lookupatts=['customer_id'],
        targetconnection=conn_wrapper
    )
    
    count = 0
    print("  Loading dim_customers...")
    for row in source:
        row = dict(row)
        row['country'] = row['country'] if row['country'] else 'n/a'
        row['gender'] = row['gender'] if row['gender'] else 'n/a'
        
        dim_customers.insert(row)
        count += 1
        
        if count % 5000 == 0:
            print(f"    Loaded {count:,} customers...")
    
    conn_wrapper.commit()
    print(f"  ✓ Loaded {count} rows into gold.dim_customers")
    return count


def get_customer_key_lookup(conn):
    """Get customer_id to customer_key mapping for fact table loading"""
    cur = conn.cursor()
    cur.execute("SELECT customer_id, customer_key FROM gold.dim_customers")
    lookup = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    return lookup
