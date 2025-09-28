# Task 2: A Line Plot with Pandas

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

with sqlite3.connect("../db/lesson.db") as conn:
    query = """
    SELECT o.order_id, 
           SUM(l.quantity * p.price) AS total_price
    FROM orders o
    JOIN line_items l ON o.order_id = l.order_id
    JOIN products p ON l.product_id = p.product_id
    GROUP BY o.order_id;
    """
    
    df = pd.read_sql_query(query, conn)
    
    df['cumulative'] = df['total_price'].cumsum()
    
    df.plot(x='order_id', y='cumulative', kind='line', title='Cumulative Revenue vs Order ID', color='navy', xlabel='Order ID', ylabel='Cumulative Revenue', grid=True)
    plt.show()
    
    