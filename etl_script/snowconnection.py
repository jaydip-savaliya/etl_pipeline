import snowflake.connector
import csv
import pandas as pd

account = 'yarglpn-dl46840'
username = 'jaydeep700'
password = 'Jsavaliya@012024'
database = 'SNOWFLAKE_SAMPLE_DATA'
schema = 'TPCH_SF1'

conn = snowflake.connector.connect(
    user=username,
    password=password,
    account=account,
    database=database,
    schema=schema
)

sql = '''
SELECT * FROM supplier;
'''

cursor = conn.cursor()

cursor.execute(sql)

rows = cursor.fetchall()

output_file = '/home/mihir/Downloads/ETL Pipeline/supplier_data.csv'

with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f)

    column_names = [desc[0] for desc in cursor.description]
    writer.writerow(column_names)
    
    writer.writerows(rows)

print(f'Data dumped successfully to {output_file}')

cursor.close()
conn.close()
