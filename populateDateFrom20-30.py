import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

# MySQL connection config
user = 'root'
password = '123456'
host = 'localhost'
port = '3306'
database = 'ecommerceDW'

# Create the SQLAlchemy engine
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

# Generate the date range
start_date = datetime(2020, 1, 1)
end_date = datetime(2030, 12, 31)
dates = pd.date_range(start=start_date, end=end_date)

# Create the date dimension DataFrame
df = pd.DataFrame({
    'date_id': dates.strftime('%Y%m%d').astype(int),   # e.g. 20200101
    'full_date': dates.date,
    'day': dates.day,
    'month': dates.month,
    'quarter': dates.quarter,
    'year': dates.year,
    'weekday_name': dates.strftime('%A')
})

# Create the table if it doesn't exist
create_table_sql = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    quarter INT,
    year INT,
    weekday_name VARCHAR(10)
);
"""

with engine.connect() as conn:
    conn.execute(text(create_table_sql))
    conn.commit()

# Insert data into dim_date table
df.to_sql('dim_date', con=engine, if_exists='append', index=False)
print("Date dimension inserted successfully.")
