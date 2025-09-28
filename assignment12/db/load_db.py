import pandas as pd
import sqlalchemy as sa
import sqlite3
import os

# --- Ensure db directory exists ---
DB_DIR = os.path.join(os.path.dirname(__file__), "db")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "lesson.db")

# --- Recreate DB if already exists ---
if os.path.exists(DB_PATH):
    answer = input("The database exists.  Do you want to recreate it (y/n)? ")
    if answer.lower() != 'y':
        exit(0)
    os.remove(DB_PATH)

# --- Create tables ---
with sqlite3.connect(DB_PATH, isolation_level='IMMEDIATE') as conn:
    conn.execute("PRAGMA foreign_keys = 1")
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,          
        customer_name TEXT,
        contact TEXT,
        street TEXT,
        city TEXT,
        postal_code TEXT,
        country TEXT,
        phone TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        employee_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        phone TEXT      
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT,
        price REAL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS line_items (
        line_item_id INTEGER PRIMARY KEY,
        order_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        FOREIGN KEY(order_id) REFERENCES orders(order_id),
        FOREIGN KEY(product_id) REFERENCES products(product_id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        employee_id INTEGER,
        date TEXT,
        FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY(employee_id) REFERENCES employees(employee_id)
    )
    """)

# --- Load CSV data into database ---
engine = sa.create_engine(f"sqlite:///{DB_PATH}")

tables = ["customers", "employees", "products", "orders", "line_items"]

for table in tables:
    csv_file = os.path.join(os.path.dirname(__file__), "..", "csv", f"{table}.csv")
    if not os.path.exists(csv_file):
        print(f"⚠️ Skipping {table}: CSV not found at {csv_file}")
        continue
    data = pd.read_csv(csv_file, sep=',')
    data.to_sql(table, engine, if_exists='append', index=False)

print(" Database initialized and populated at:", DB_PATH)
