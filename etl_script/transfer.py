import pandas as pd
from sqlalchemy import create_engine
import psycopg2
engine = create_engine('postgresql://mihir:secret@localhost:5432/destination_db')
df = pd.read_csv("orders_data.csv",delimiter = ',')
df.to_sql('orders_data',engine,schema="etl", if_exists='replace',index=False)