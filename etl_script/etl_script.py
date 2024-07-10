import snowflake.connector
import os
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import csv

engine = create_engine('postgresql://postgres:secret@192.168.2.75:5434/destination_db')
account = 'yarglpn-dl46840' 
username = 'jaydeep700'
password = 'Jsavaliya@012024'
warehouse = 'dbt_wh'
database = 'dbt_db'
schema = 'dbt_schema'

conn = snowflake.connector.connect(
    account=account,
    user=username,
    password=password,
    database=database,
    schema=schema,
    warehouse=warehouse,
    role='DBT_ROLE'
)

tables = os.listdir("/opt/airflow/dbt/models/Tables")
list_of_table = []
for table in tables:
    if table.endswith(".sql"):
        list_of_table.append(f"{table[:-4]}")  

for i in list_of_table:
    cursor = conn.cursor()
    query = f'''
            select * from {i}
            '''
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows,columns=columns)
    df.to_sql(f'{i}',engine,schema="public", if_exists='replace',index=False)
    cursor.close()
conn.close()