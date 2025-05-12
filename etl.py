import pandas as pd
from sqlalchemy import create_engine
import pymysql
import os
from dotenv import load_dotenv
from sqlalchemy import text

# Load environment variables
load_dotenv()

# ========== CONFIG ==========
# MySQL Source (ecommerce)
source_config = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT")),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": "ecommerce"
}

# MySQL Destination (EcommerceDW)
destination_config = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT")),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": "EcommerceDW"
}

# ========== CONNECTION UTILS ==========
def get_source_conn():
    return pymysql.connect(
        host=source_config["host"],
        user=source_config["user"],
        password=source_config["password"],
        db=source_config["database"],
        port=source_config["port"]
    )

def get_destination_engine():
    return create_engine(
        f"mysql+pymysql://{destination_config['user']}:{destination_config['password']}@"
        f"{destination_config['host']}:{destination_config['port']}/{destination_config['database']}"
    )

# ========== ETL PROCESS ==========
def extract_data(query, conn):
    return pd.read_sql(query, conn)

def transform_dates(df, column):
    # Convert DATE to INT yyyymmdd
    df[column] = pd.to_datetime(df[column])
    df[column + "_id"] = df[column].dt.strftime("%Y%m%d").astype(int)
    return df

def load_data(df, table_name, engine, if_exists='replace', primary_key='id'):
    with engine.connect() as conn:
        if if_exists == 'replace': 
            # Create a temporary table to hold the new data
            temp_table_name = f"{table_name}_temp"
            df.to_sql(temp_table_name, engine, if_exists=if_exists, index=False)

            # Insert only rows that do not already exist in the target table
            insert_query = f"""
            INSERT INTO {table_name}
            SELECT * FROM {temp_table_name}
            WHERE {primary_key} NOT IN (SELECT {primary_key} FROM {table_name});
            """
            conn.execute(text(insert_query))

            # Drop the temporary table
            conn.execute(text(f"DROP TABLE {temp_table_name};"))
        else:
            # Load data directly into the target table
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        

def run_etl():
    # Connect to source and destination
    source_conn = get_source_conn()
    destination_engine = get_destination_engine()
    # DIM: PRODUCTS
    products_query = "SELECT id AS product_id, name, category, pricing AS price FROM products"
    products = extract_data(products_query, source_conn)
    products["brand"] = "ptit"  # Add a static brand column
    # Reorder columns to place 'brand' to the left of 'pricing'
    products = products[["product_id", "name", "category", "brand", "price"]]
    load_data(products, "dim_products", destination_engine,primary_key="product_id")

    # DIM: CUSTOMERS
    customers_query = "SELECT id AS customer_id, name, email, address_id FROM customers"
    customers = extract_data(customers_query, source_conn)
    customers["phone"] = "0123456789"  # Add a static phone column
    # Reorder columns to place 'phone' to the left of 'email'
    customers = customers[["customer_id", "name", "email", "phone", "address_id"]]
    load_data(customers, "dim_customers", destination_engine,primary_key="customer_id")

    # DIM: ADDRESSES
    addresses_query = "SELECT id AS address_id, city, region AS state, 'Vietnam' AS country, '' AS zipcode FROM addresses"
    addresses = extract_data(addresses_query, source_conn)
    load_data(addresses, "dim_addresses", destination_engine,primary_key="address_id")

    # FACT: ORDERS
    start_date = '2020-12-04'
    end_date = '2026-12-05'
    orders_query = f"""
    SELECT 
        o.customer_id,
        op.product_id,
        ad.id AS address_id,
        DATE(o.created_at) AS order_date,
        SUM(op.quantity) AS quantity,
        AVG(op.unit_price) AS price
    FROM orders o
    JOIN order_product op ON o.id = op.order_id
    JOIN addresses ad ON o.address_id = ad.id
    GROUP BY o.customer_id, op.product_id, DATE(o.created_at), ad.id
    HAVING order_date >= '{start_date}' AND order_date <= '{end_date}'
"""
    orders = extract_data(orders_query, source_conn)
    orders = transform_dates(orders, "order_date")
    fact_orders = orders[[
        "customer_id", "product_id","address_id", "order_date_id", "quantity", "price"
    ]]
    load_data(fact_orders, "fact_orders", destination_engine,if_exists='append')

    print("ETL process completed successfully.")

# ========== MAIN ==========
if __name__ == "__main__":
    run_etl()