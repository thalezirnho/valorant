# Assets Only

import os
import requests
import pandas as pd

from google.cloud import bigquery

path = 'credentials/service_account.json'
client_gbq = bigquery.Client.from_service_account_json(path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(path)

# Collectiong all Maps 
endpoint = 'maps'

response = requests.get(f'https://valorant-api.com/v1/{endpoint}')

maps_list = response.json()['data']

maps = pd.DataFrame()

for map_ in maps_list:
    
    uuid = map_.get('uuid')
    displayName = map_.get('displayName')
    
    
    if displayName != 'The Range':
        callouts = map_.get('callouts')
        callouts = pd.json_normalize(callouts, sep='_')

        callouts['uuid'] = uuid
        callouts['displayName'] = displayName

        callouts = callouts[['uuid', 'displayName', 'regionName', 'superRegionName', 'location_x', 'location_y']]
        
        maps = pd.concat([maps, callouts], axis=0)
        
        try:
            del callouts
        except:
            pass

maps.reset_index(inplace=True, drop=True)

maps['location_x'] = maps['location_x'].astype(float).round(1)
maps['location_y'] = maps['location_y'].astype(float).round(1)

maps_schema = [
    {'name': 'uuid',            'type': 'STRING'},
    {'name': 'displayName',     'type': 'STRING'},
    {'name': 'regionName',      'type': 'STRING'},
    {'name': 'superRegionName', 'type': 'STRING'},
    {'name': 'location_x',      'type': 'FLOAT'},
    {'name': 'location_y',      'type': 'FLOAT'}
]

maps.to_gbq(
          f'tabular_valorant_assets.{endpoint}',
          'thales-1615931464192',
          if_exists='replace',
          table_schema=maps_schema
        ) 