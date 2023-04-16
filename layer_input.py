# Libraries
import os
import json
import time
import hashlib
import logging
import requests
import pandas_gbq

from pandas import DataFrame, to_datetime, concat
from datetime import datetime, timedelta
from google.cloud import storage, bigquery


def setup_dir(name: str):
    # Function to setup a directory, if it don't exist
    if not os.path.exists(name):
        os.makedirs(name)
    else:
        pass

    
def valorant_layer_input() -> list:

    exec_time = datetime.now()
    # Converts to Brazil time
    if time.tzname[0] == 'UTC':
        exec_time = exec_time - timedelta(hours=3)
        
    # Starting logs
    exec_time = exec_time.strftime('%Y%m%d%H%m%S')
    
    logging.basicConfig(filename=f'input_log.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

    # Diretory validations
    logging.info('Validating local directory existence...')
    # Folder to store Matchs
    setup_dir('match')
    # Folder to store utilities
    setup_dir('utils')
    

    # Auth in Google Cloud Services
    logging.info('Authenticating in GCP...')
    path = 'credentials/service_account.json' # TO DO: user env variable
    # Storage
    client = storage.Client.from_service_account_json(path)
    bucket_name = "valorant_data"
    bucket = client.bucket(bucket_name)

    # Bigquery
    client_gbq = bigquery.Client.from_service_account_json(path)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(path)

    logging.info('End of authentication...')

    # Get all stored match IDs
    logging.info('Retrieving all bucket matchids...')
    folder_name = "match"
    folder = bucket.blob(folder_name)

    stored_matchs = []

    for blob in bucket.list_blobs():
        stored_matchs.append(blob.name.replace('match/', '').replace('.json', ''))
    
    # Declare execution time
    logging.info('Declaring execution timing...')
    date_time = datetime.now()

    if time.tzname[0] == 'UTC':
        date_time = date_time - timedelta(hours=3)

    dt_extract = date_time.strftime('%Y-%m-%dT%H:%m:%S')
    date_time = date_time.strftime('%Y%m%d_%H%m%S')

    # Target Users
    logging.info('Listing all target users...')
    
    if not os.path.exists("utils/json_users.json"):
        blob = bucket.blob(f"utils/json_users.json")
        blob.download_to_filename(f'utils/json_users.json')
        
    with open('utils/json_users.json') as f:
        users = json.load(f)
        
    # Users file sample:
    # users = {
    #     "name": "tag"
    # }


    # Declare empty list to store new match Ids
    session_matchs = []

    logging.info('Getting all last 5 games per user...')

    for name, tag in users.items():

        # Match History
        endpoint = 'matches'
        response = requests.get(f"https://api.henrikdev.xyz/valorant/v3/{endpoint}/na/{name}/{tag}?filter=competitive")

        if response.status_code == 200:

            for match in response.json()['data']:
                
                matchid = match['metadata']['matchid']

                # Store the match if it does not exist in Bucket and not exist in current execution
                if matchid not in stored_matchs and matchid not in session_matchs:
                    
                    session_matchs.append(matchid)
                    session_matchs = list(set(session_matchs))

                    # Save file locally
                    file_name = f'match/{matchid}.json'

                    with open(f'{file_name}', 'w') as f:
                        json.dump(match, f)

                    blob = bucket.blob(f"{file_name}")
                    blob.upload_from_filename(file_name)
       
        else:
            # If status_code != 200, ingest data in BQ (palliative)
            error_id = hashlib.md5((dt_extract+name+tag).encode('utf-8')).hexdigest()
            df_error = DataFrame([{'id': error_id, 'datatime': dt_extract, 'name': name, 'tag': tag, 'endpoint': endpoint, 'status': response.status_code}])

            df_error['datetime'] = to_datetime(df_error['datetime'])

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


    # Get all matches stored after run in BQ
    logging.info('Ingest all information data from this run...')

    folder = bucket.blob(folder_name)

    df_matches = DataFrame()

    for blob in bucket.list_blobs():
        df_matches = concat([df_matches, DataFrame([blob.name.replace('match/', '').replace('.json', '')])])

    df_matches.columns = ['matchid']

    df_matches['datetime'] = dt_extract

    df_matches = df_matches[['datetime', 'matchid']]
    df_matches.reset_index(inplace=True, drop=True)

    df_matches['datetime'] = to_datetime(df_matches['datetime'])

    schema = [
        {'name': 'datetime', 'type': 'DATETIME'},
        {'name': 'matchid', 'type': 'STRING'}
    ]

    logging.info('Ingesting data in Google BQ...')

    df_matches.to_gbq(
        f'logs_valorant.match_in_storage',
        'thales-1615931464192',
        if_exists='append',
        table_schema=schema
    )   

    # Save execution Log in bucket
    try:
        blob = bucket.blob(f'/logs/input_log_{exec_time}.txt')
        blob.upload_from_filename(f'input_log_{exec_time}.txt')
        
    except:
        print('Unable to upload logs')
    
    # Deleting all files .txt logs
    for file_to_delete in [file for file in os.listdir() if file.endswith('.txt') and '_log' in file]:
        print(f'Deleting {file_to_delete}')
        os.remove(file_to_delete)
    
    # Return all new matches for tab
    return session_matchs