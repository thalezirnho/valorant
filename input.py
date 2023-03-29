# Libraries
import os
import json
import time
import hashlib
import requests
import pandas as pd
import pandas_gbq
from datetime import timedelta
from google.cloud import storage, bigquery

from datetime import datetime


# Auth
path = 'credentials/service_account.json'
client = storage.Client.from_service_account_json(path)

bucket_name = "valorant_data"

bucket = client.bucket(bucket_name)

client_gbq = bigquery.Client.from_service_account_json(path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(path)

# Get all stored matchs
folder_name = "match"

folder = bucket.blob(folder_name)

stored_matchs = []

for blob in bucket.list_blobs():
    stored_matchs.append(blob.name.replace('match/', '').replace('.json', ''))
    
# Declare execution time
date_time = datetime.now()
                    
if time.tzname[0] == 'UTC':
    date_time = date_time - timedelta(hours=3)

year = date_time.strftime('%Y')
year_month = date_time.strftime('%Y%m')
day = date_time.strftime('%d')
timestamp = int(date_time.timestamp())
dt_extract = date_time.strftime('%Y-%m-%dT%H:%m:%S')

date_time = date_time.strftime('%Y%m%d_%H%m%S')


# Target Users
users = {
    "viikset": "BR1",
    "TADALA": "TADAL",
    "DIGAOTJS": "BR1",
    "mBirth": "BR1",
    "Kubata": "4881"
}

puuids = {
    "viikset": "f2f7c867-27ba-5291-a23c-bbdcfc079e76",
    "TADALA": "35a2368e-586f-53ec-8ee8-ea9ebdca7d6f",
    "DIGAOTJS": "2c51a2e4-0dcf-53f5-ae85-cd4d7d79f663",
    "mBirth": "6c241ab0-1c2f-5d5b-8908-4e809df8f6ca",
    "Kubata": "b2310858-1741-5d6f-8499-cb1f85bd182a"
}

# Layer Input
session_matchs = []

for name, tag in users.items():
    
    # Match History
    endpoint = 'matches'
    response = requests.get(f"https://api.henrikdev.xyz/valorant/v3/{endpoint}/na/{name}/{tag}?filter=competitive")
    
    if response.status_code != 200:
        
        error_id = hashlib.md5((dt_extract+name+tag).encode('utf-8')).hexdigest()
        df_error = pd.DataFrame([{'id': error_id, 'datatime': dt_extract, 'name': name, 'tag': tag, 'endpoint': endpoint, 'status': response.status_code}])
        
        df_error['datetime'] = pd.to_datetime(df_error['datetime'])

        schema = [
            {'name': 'id', 'type': 'STRING'},
            {'name': 'datetime', 'type': 'DATETIME'},
            {'name': 'name', 'type': 'STRING'},
            {'name': 'tag', 'type': 'STRING'},
            {'name': 'endpoint', 'type': 'STRING'},
            {'name': 'status', 'type': 'INTEGER'}
        ]
        
        df_error.to_gbq(
            f'logs_valorant.api_error_logs',
            'thales-1615931464192',
            if_exists='append',
            table_schema=schema
        )   
        
    else:

        for match in response.json()['data']:

            matchid = match['metadata']['matchid']

            if matchid not in stored_matchs and matchid not in session_matchs:
                
                session_matchs.append(matchid)
                session_matchs = list(set(session_matchs))

                # Save file
                file_name = f'match/{matchid}.json'

                with open(f'{file_name}', 'w') as f:
                    json.dump(match, f)

                blob = bucket.blob(f"{file_name}")
                blob.upload_from_filename(file_name)


                
# Get all matches stored after run

folder = bucket.blob(folder_name)

df_matches = pd.DataFrame()

for blob in bucket.list_blobs():
    df_matches = pd.concat([df_matches, pd.DataFrame([blob.name.replace('match/', '').replace('.json', '')])])

df_matches.columns = ['matchid']

df_matches['datetime'] = dt_extract

df_matches = df_matches[['datetime', 'matchid']]
df_matches.reset_index(inplace=True, drop=True)

df_matches['datetime'] = pd.to_datetime(df_matches['datetime'])

schema = [
    {'name': 'datetime', 'type': 'DATETIME'},
    {'name': 'matchid', 'type': 'STRING'}
]


df_matches.to_gbq(
    f'logs_valorant.match_in_storage',
    'thales-1615931464192',
    if_exists='append',
    table_schema=schema
)   