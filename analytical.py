import os
from google.cloud import bigquery

path = 'credentials/service_account.json'
client_gbq = bigquery.Client.from_service_account_json(path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(path)

project = 'thales-1615931464192'
dataset = 'analytical_valorant'

# Read all .sql files in sql folder
queries = [query for query in os.listdir('./sql') if query.endswith('.sql')]

for query in queries:
    
    table_name = query[:-4]
    #print(f'Creating {table_name}...')
    
    with open(f'sql/{query}', 'r') as f:
        sqlFile = f.read()
    
    
    ddlFile = f'CREATE OR REPLACE TABLE {project}.{dataset}.{table_name} AS (\n{sqlFile})'
    
    response = client_gbq.query(ddlFile)
    if response.errors:
        print(f'Error in {table_name}...')