import time
from datetime import datetime
from pygrametl import ConnectionWrapper

from db import (
    get_connection, 
    create_silver_tables, 
    truncate_silver_tables,
    create_gold_tables,
    truncate_gold_tables
)

from sources.customers import load_customers
from sources.products import load_products
from sources.sales import load_sales
from sources.erp_customer import load_erp_customers
from sources.erp_location import load_erp_locations
from sources.erp_category import load_erp_categories

from dimensions.dim_customers import load_dim_customers
from dimensions.dim_products import load_dim_products
from dimensions.fact_sales import load_fact_sales


def run_silver_etl(conn_wrapper, source_conn):
    """Run Silver layer ETL (Bronze → Silver)"""
    print("\n" + "="*60)
    print("   SILVER LAYER ETL (Bronze → Silver)")
    print("="*60)
    
    results = {}
    
    print("\n[Silver 1/6] Loading CRM Customer data...")
    results['crm_customers'] = load_customers(conn_wrapper, source_conn)
    
    print("\n[Silver 2/6] Loading CRM Product data...")
    results['crm_products'] = load_products(conn_wrapper, source_conn)
    
    print("\n[Silver 3/6] Loading CRM Sales data...")
    results['crm_sales'] = load_sales(conn_wrapper, source_conn)
    
    print("\n[Silver 4/6] Loading ERP Customer demographics...")
    results['erp_customers'] = load_erp_customers(conn_wrapper, source_conn)
    
    print("\n[Silver 5/6] Loading ERP Location data...")
    results['erp_locations'] = load_erp_locations(conn_wrapper, source_conn)
    
    print("\n[Silver 6/6] Loading ERP Product categories...")
    results['erp_categories'] = load_erp_categories(conn_wrapper, source_conn)
    
    return results


def run_gold_etl(conn_wrapper, source_conn, target_conn):
    """Run Gold layer ETL (Silver → Gold Star Schema)"""
    print("\n" + "="*60)
    print("   GOLD LAYER ETL (Silver → Gold Star Schema)")
    print("="*60)
    
    results = {}
    
    print("\n[Gold 1/3] Loading dim_customers...")
    results['dim_customers'] = load_dim_customers(conn_wrapper, source_conn)
    
    print("\n[Gold 2/3] Loading dim_products...")
    results['dim_products'] = load_dim_products(conn_wrapper, source_conn)
    
    print("\n[Gold 3/3] Loading fact_sales...")
    results['fact_sales'] = load_fact_sales(conn_wrapper, source_conn, target_conn)
    
    return results


def run_full_etl():
    """Execute the complete ETL pipeline from Bronze to Silver to Gold"""
    start_time = time.time()
    
    print("\n" + "="*60)
    print("   DATA WAREHOUSE ETL PIPELINE")
    print("   Bronze → Silver → Gold (Star Schema)")
    print(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Get database connections
    print("\n[Setup] Connecting to database...")
    source_conn = get_connection()
    target_conn = get_connection()
    
    silver_results = {}
    gold_results = {}
    
    try:
        # =============================================
        # SILVER LAYER
        # =============================================
        print("\n[Setup] Creating Silver tables...")
        create_silver_tables(target_conn)
        
        print("\n[Setup] Truncating Silver tables...")
        truncate_silver_tables(target_conn)
        
        target_conn.cursor().execute("SET search_path = 'silver'")
        conn_wrapper = ConnectionWrapper(target_conn)
        
        silver_results = run_silver_etl(conn_wrapper, source_conn)
        
        conn_wrapper.commit()
        conn_wrapper.close()

        target_conn = get_connection()
        
        print("\n[Setup] Creating Gold tables (Star Schema)...")
        create_gold_tables(target_conn)
        
        print("\n[Setup] Truncating Gold tables...")
        truncate_gold_tables(target_conn)
        
        target_conn.cursor().execute("SET search_path = 'gold'")
        conn_wrapper = ConnectionWrapper(target_conn)
        
        gold_results = run_gold_etl(conn_wrapper, source_conn, target_conn)
        
        conn_wrapper.commit()
        conn_wrapper.close()
        
    except Exception as e:
        print(f"\n❌ Error during ETL: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        source_conn.close()
        target_conn.close()
    

    elapsed_time = time.time() - start_time
    print("\n" + "="*60)
    print("   ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"   Total time: {elapsed_time:.2f} seconds")
    print("="*60)
    
    print("\n📊 SILVER LAYER SUMMARY")
    print("-" * 40)
    for table, count in silver_results.items():
        print(f"  {table}: {count:,} rows")
    
    print("\n⭐ GOLD LAYER SUMMARY (Star Schema)")
    print("-" * 40)
    for table, count in gold_results.items():
        print(f"  {table}: {count:,} rows")
    
    print()


def run_gold_only():
    """Run only the Gold layer ETL (assumes Silver is already loaded)"""
    start_time = time.time()
    
    print("\n" + "="*60)
    print("   GOLD LAYER ETL ONLY (Star Schema)")
    print(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    source_conn = get_connection()
    target_conn = get_connection()
    
    try:
        print("\n[Setup] Creating Gold tables (Star Schema)...")
        create_gold_tables(target_conn)
        
        print("\n[Setup] Truncating Gold tables...")
        truncate_gold_tables(target_conn)
        
        target_conn.cursor().execute("SET search_path = 'gold'")
        conn_wrapper = ConnectionWrapper(target_conn)
        
        gold_results = run_gold_etl(conn_wrapper, source_conn, target_conn)
        
        conn_wrapper.commit()
        conn_wrapper.close()
        
    finally:
        source_conn.close()
        target_conn.close()
    
    elapsed_time = time.time() - start_time
    print("\n" + "="*60)
    print("   GOLD ETL COMPLETED!")
    print(f"   Total time: {elapsed_time:.2f} seconds")
    print("="*60)
    
    print("\n⭐ GOLD LAYER SUMMARY")
    print("-" * 40)
    for table, count in gold_results.items():
        print(f"  {table}: {count:,} rows")
    print()


def run_single_etl(table_name: str):
    """Run ETL for a single table"""
    print(f"\n Running ETL for: {table_name}")
    
    source_conn = get_connection()
    target_conn = get_connection()
    target_conn.cursor().execute("SET search_path = 'silver'")
    conn_wrapper = ConnectionWrapper(target_conn)
    
    try:
        etl_functions = {
            'crm_cust_info': load_customers,
            'crm_prd_info': load_products,
            'crm_sales_details': load_sales,
            'erp_cust_az12': load_erp_customers,
            'erp_loc_a101': load_erp_locations,
            'erp_px_cat_g1v2': load_erp_categories
        }
        
        if table_name not in etl_functions:
            print(f"❌ Unknown table: {table_name}")
            print(f"Available tables: {list(etl_functions.keys())}")
            return
        
        cur = target_conn.cursor()
        cur.execute(f"TRUNCATE TABLE silver.{table_name};")
        target_conn.commit()
        
        etl_functions[table_name](conn_wrapper, source_conn)
        
        conn_wrapper.commit()
        conn_wrapper.close()
        print(f"✓ ETL completed for {table_name}")
        
    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    run_full_etl()
