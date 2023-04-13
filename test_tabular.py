#!/usr/bin/env python
# coding: utf-8

# In[4]:


import os
import json
import uuid
import time
import hashlib
import logging
import requests
import numpy as np
import pandas as pd
import pandas_gbq
import inspect
from datetime import timedelta, datetime
from google.cloud import storage, bigquery

from datetime import datetime

import warnings
warnings.filterwarnings("ignore")


# In[5]:


pd.set_option('display.max_columns', 100)


# In[6]:


exec_time = datetime.now().strftime('%Y%m%d%H%m%S')


# In[7]:


logging.basicConfig(filename=f'tabular_logs_{exec_time}.txt', level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')


# In[8]:


logging.info('Definindo função build_schema')
def build_schema(df):    

    temp_schema = pd.DataFrame(df.dtypes, columns=['dtype']).reset_index()

    schema_list = []

    for i, row in temp_schema.iterrows():
        if str(row['dtype']).upper() == 'INT64':
            dtype = 'INTEGER'
        elif str(row['dtype']).upper() == 'FLOAT64':
            dtype = 'FLOAT'
        elif str(row['dtype']).upper() == 'OBJECT':
            dtype = 'STRING'
        elif str(row['dtype']).upper() == 'BOOL':
            dtype = 'BOOLEAN'

        schema_list.append({'name': row['index'], 'type': dtype})

    return schema_list

logging.info('Definindo função retrieve_name')
def retrieve_name(var):
    callers_local_vars = inspect.currentframe().f_back.f_locals.items()
    return [var_name for var_name, var_val in callers_local_vars if var_val is var][0]


logging.info('Definindo função reinforce_col_dtype')
def reinforce_col_dtype(df, my_dict):
    
    #df = df.fillna(value=None)
    
    df_name = [var for var in globals() if globals()[var] is df][0]
    
    if df_name in my_dict:
        
        metadata_schema = my_dict[df_name]
        
        for dtype in metadata_schema.keys():

            cols = metadata_schema[dtype]

            if dtype == 'int':
                dtype = 'Int64'
            elif dtype == 'float':
                dtype = 'Float64'
            elif dtype == 'bool':
                dtype = bool
            else:
                dtype = str

            for col in cols:
                df[col] = df[col].astype(dtype, errors='ignore')
    else:
        pass
    
    df.replace('nan', None, inplace=True)

    return df


# In[9]:


logging.info('Autenticando no Storage')
path = 'credentials/service_account.json'
client = storage.Client.from_service_account_json(path)


bucket_name = "valorant_data"
logging.info('Definindo o Bucket')
bucket = client.bucket(bucket_name)


logging.info('Autenticando no Bigquery')
client_gbq = bigquery.Client.from_service_account_json(path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(path)


# Get all stored matchs
logging.info('Recuperando todos os jogos direto no bucket')
folder_name = "match"

folder = bucket.blob(folder_name)

matches = []

logging.info('Criando a lista de jogos')
for blob in bucket.list_blobs(prefix=folder_name):
    matches.append(blob.name.replace('match/', '').replace('.json', ''))


# In[10]:


logging.info('Lendo dados do bigquery para identificar quais matchid já existem na camada tabular')
matches_in_bq = []
query_job = client_gbq.query("SELECT DISTINCT matchid FROM `thales-1615931464192.tabular_valorant_match.metadata`")
results = query_job.result()

for row in results:
    matches_in_bq.append(row[0])

# Only new match
matches = [m for m in matches if m not in matches_in_bq]


# In[12]:


matches = [matches_in_bq[0]]


# In[ ]:


if not os.path.exists("json_metadata.json"):
    blob = bucket.blob(f"utils/json_metadata.json")
    blob.download_to_filename(f'json_metadata.json')

# Reading metadata
logging.info('Lendo os metadados das colunas de todos os dataframes')
with open('json_metadata.json') as f:
    data = json.load(f)


# In[ ]:


matches


# In[ ]:


count = 1
logging.info('Início da iteração entre os jogos')

print(f"{len(matches)} will be ingest...\n")

for matchid in matches:
    print(f'_____________\nStarting map number {count}...')

    print(matchid)
    logging.info(f'Recuperando o arquivo do {matchid} formato json')
    blob = bucket.blob(f"match/{matchid}.json")
    #blob.download_to_filename(f'/home/thalesfollador/valorant/match/{matchid}.json')
    blob.download_to_filename(f'match/{matchid}.json')
    print('Download concluido')
    logging.info('Download concluído')
    
    logging.info(f'Lendo o {matchid}.json da pasta local match')
    with open(f'match/{matchid}.json', 'r') as f:
        match = json.load(f)

    #metadata = match.pop('metadata')
    #players = match.pop('players')
    #teams = match.pop('teams')
    #rounds = match.pop('rounds')
    #kills = match.pop('kills')
    logging.info(f'Criando os subarquivos baseado nas chaves')
    metadata = match['metadata']
    players = match['players']
    teams = match['teams']
    rounds = match['rounds']
    kills = match['kills']

    ## Metadata
    df_metadata = pd.json_normalize(metadata)

    ## Players
    #all_players = players.pop('all_players')
    all_players = players['all_players']
    #red = players.pop('red')
    #blue = players.pop('blue')

    ### All Players
    # all_players = pd.concat([red, blue])

    json_players_keys = ['session_playtime', 'behavior', 'platform', 'ability_casts', 'assets', 'stats', 'economy']

    df_player = pd.DataFrame()
    df_player_ability_casts = pd.DataFrame()
    df_player_assets = pd.DataFrame()
    df_player_behavior = pd.DataFrame()
    df_player_economy = pd.DataFrame()
    df_player_platform = pd.DataFrame()
    df_player_session_playtime = pd.DataFrame()
    df_player_stats = pd.DataFrame()


    logging.info(f'Iterando a seção de Players')
    for player in all_players:

        puuid = player['puuid']

        for key in json_players_keys:
            logging.info(f'Tranformando subseções em variáveis')

            exec(f"player_{key} = player.pop('{key}')")
            #exec(f"player_{key} = player['{key}']")
            
        logging.info(f'Criando o df_player')
        temp_player = pd.json_normalize(player)
        temp_player['matchid'] = matchid
        df_player = pd.concat([df_player, temp_player])

        logging.info(f'Criando o df_player_ability_casts')
        temp_player_ability_casts = pd.json_normalize(player_ability_casts)
        temp_player_ability_casts['matchid'] = matchid
        temp_player_ability_casts['puuid'] = puuid
        df_player_ability_casts = pd.concat([df_player_ability_casts, temp_player_ability_casts])

        logging.info(f'Criando o df_player_assets')
        temp_player_assets = pd.json_normalize(player_assets, sep='_')
        temp_player_assets['matchid'] = matchid
        temp_player_assets['puuid'] = puuid
        df_player_assets = pd.concat([df_player_assets, temp_player_assets])

        logging.info(f'Criando o df_player_behavior')
        temp_player_behavior = pd.json_normalize(player_behavior, sep='_')
        temp_player_behavior['matchid'] = matchid
        temp_player_behavior['puuid'] = puuid
        df_player_behavior = pd.concat([df_player_behavior, temp_player_behavior])

        logging.info(f'Criando o df_player_economy')
        temp_player_economy = pd.json_normalize(player_economy, sep='_')
        temp_player_economy['matchid'] = matchid
        temp_player_economy['puuid'] = puuid
        df_player_economy = pd.concat([df_player_economy, temp_player_economy])

        logging.info(f'Criando o df_player_platform')
        temp_player_platform = pd.json_normalize(player_platform, sep='_')
        temp_player_platform['matchid'] = matchid
        temp_player_platform['puuid'] = puuid
        df_player_platform = pd.concat([df_player_platform, temp_player_platform])

        logging.info(f'Criando o df_player_session_playtime')
        temp_player_session_playtime = pd.json_normalize(player_session_playtime, sep='_')
        temp_player_session_playtime['matchid'] = matchid
        temp_player_session_playtime['puuid'] = puuid
        df_player_session_playtime = pd.concat([df_player_session_playtime, temp_player_session_playtime])

        logging.info(f'Criando o ')
        temp_player_stats = pd.json_normalize(player_stats, sep='_')
        temp_player_stats['matchid'] = matchid
        temp_player_stats['puuid'] = puuid
        df_player_stats = pd.concat([df_player_stats, temp_player_stats])

    df_player.reset_index(inplace=True, drop=True)
    df_player_ability_casts.reset_index(inplace=True, drop=True)
    df_player_assets.reset_index(inplace=True, drop=True)
    
    df_player_behavior.reset_index(inplace=True, drop=True)
    df_player_behavior.iloc[:,:4] = df_player_behavior.iloc[:,:4].astype(int)
    
    df_player_economy.reset_index(inplace=True, drop=True)
    df_player_platform.reset_index(inplace=True, drop=True)
    df_player_session_playtime.reset_index(inplace=True, drop=True)
    df_player_stats.reset_index(inplace=True, drop=True)

    ## Teams
    logging.info(f'Criando o df_teams')
    df_teams = pd.json_normalize(teams, sep='_')
    df_teams['matchid'] = matchid

    ## Rounds
    json_round_keys = ['plant_events', 'defuse_events', 'player_stats']

    round_number = 0
    df_round_plant_events = pd.DataFrame()
    df_player_locations_on_plant = pd.DataFrame()
    df_round_defuse_events = pd.DataFrame()
    df_player_locations_on_defuse = pd.DataFrame()
    df_round_player_stats = pd.DataFrame()
    df_round_player_damage_events = pd.DataFrame()
    
    df_round = pd.DataFrame()

    logging.info(f'Iterando os rounds')
    for rnd in rounds:

        for key in json_round_keys:
            logging.info(f'Criando subvariáveis dos rounds')
            exec(f"round_{key} = rnd.pop('{key}')")
            #exec(f"round_{key} = rnd['{key}']")
            
        logging.info(f'Criando Round ID')
        round_id = str(uuid.uuid5(uuid.NAMESPACE_OID, matchid + str(round_number)))

        # Round
        logging.info(f'Criando o df_round')
        temp_round = pd.DataFrame([rnd])
        temp_round['matchid'] = matchid
        temp_round['round_id'] = round_id
        temp_round['round'] = round_number

        df_round = pd.concat([df_round, temp_round])

        
        # Plant Events
        logging.info(f'Criando o df_round_plant_events')
        player_locations_on_plant = round_plant_events.pop('player_locations_on_plant')
        #player_locations_on_plant = round_plant_events['player_locations_on_plant']

        temp_round_plant_events = pd.json_normalize(round_plant_events, sep='_')

        temp_round_plant_events['matchid'] = matchid
        temp_round_plant_events['round_id'] = round_id
        temp_round_plant_events['round'] = round_number

        df_round_plant_events = pd.concat([df_round_plant_events, temp_round_plant_events])

        if player_locations_on_plant is not None:
            logging.info(f'Criando o df_player_locations_on_plant')

            temp_player_locations_on_plant = pd.json_normalize(player_locations_on_plant, sep='_')

            temp_player_locations_on_plant['matchid'] = matchid
            temp_player_locations_on_plant['round_id'] = round_id
            temp_player_locations_on_plant['round'] = round_number

            df_player_locations_on_plant = pd.concat([df_player_locations_on_plant, temp_player_locations_on_plant])

        # Defuse Events
        logging.info(f'Criando o df_round_defuse_events')
        player_locations_on_defuse = round_defuse_events.pop('player_locations_on_defuse')
        #player_locations_on_defuse = round_defuse_events['player_locations_on_defuse']

        temp_round_defuse_events = pd.json_normalize(round_defuse_events, sep='_')

        temp_round_defuse_events['matchid'] = matchid
        temp_round_defuse_events['round_id'] = round_id
        temp_round_defuse_events['round'] = round_number

        df_round_defuse_events = pd.concat([df_round_defuse_events, temp_round_defuse_events])


        if player_locations_on_defuse is not None:
            logging.info(f'Criando o df_player_locations_on_defuse')

            temp_player_locations_on_defuse = pd.json_normalize(player_locations_on_defuse, sep='_')

            temp_player_locations_on_defuse['matchid'] = matchid
            temp_player_locations_on_defuse['round_id'] = round_id
            temp_player_locations_on_defuse['round'] = round_number

            df_player_locations_on_defuse = pd.concat([df_player_locations_on_defuse, temp_player_locations_on_defuse])
        
        
        # Create Round Player Stats
        logging.info(f'Criando o temp_round_player_stats')
        temp_round_player_stats = pd.json_normalize(round_player_stats, sep='_')
        temp_round_player_stats['matchid'] = matchid
        temp_round_player_stats['round_id'] = round_id
        temp_round_player_stats['round'] = round_number
        
        df_round_player_stats = pd.concat([df_round_player_stats, temp_round_player_stats])
        
        for element in round_player_stats:
    
            player_puuid = element['player_puuid']
            damage_events = element['damage_events']

            if len(damage_events) >= 1:
                logging.info(f'Criando o df_round_player_damage_events')
                temp_round_player_damage_events = pd.DataFrame(damage_events)
                temp_round_player_damage_events['damager_puuid'] = player_puuid
                temp_round_player_damage_events['matchid'] = matchid
                temp_round_player_damage_events['round_id'] = round_id
                temp_round_player_damage_events['round'] = round_number

                df_round_player_damage_events = pd.concat([df_round_player_damage_events, temp_round_player_damage_events])

        
        
        round_number += 1
        
    df_round_plant_events['plant_time_in_round'].replace({None: np.nan}, inplace=True)
    df_round_plant_events.drop(['plant_location', 'planted_by'], axis=1, inplace=True)
    
    df_round_defuse_events['defuse_time_in_round'].replace({None: np.nan}, inplace=True)
    df_round_defuse_events.drop(['defuse_location', 'defused_by'], axis=1, inplace=True)
    
    df_round_player_stats['ability_casts_c_casts'].replace({None: np.nan}, inplace=True)
    df_round_player_stats['ability_casts_e_cast'].replace({None: np.nan}, inplace=True)
    df_round_player_stats['ability_casts_q_casts'].replace({None: np.nan}, inplace=True)
    df_round_player_stats['ability_casts_x_cast'].replace({None: np.nan}, inplace=True)
    df_round_player_stats.drop(['damage_events', 'kill_events'], axis=1, inplace=True)
    df_round_player_stats.reset_index(inplace=True, drop=True)
    
    df_round_player_damage_events.reset_index(inplace=True, drop=True)
    
    
    ### Kills

    df_kills = pd.DataFrame()
    df_player_locations_on_kill = pd.DataFrame()
    df_assistants = pd.DataFrame()

    logging.info(f'Iterando os eventos de kills')
    for kill in kills:

        player_locations_on_kill = kill.pop('player_locations_on_kill')
        #player_locations_on_kill = kill['player_locations_on_kill']
        assistants = kill.pop('assistants')
        #assistants = kill['assistants']

        temp_kills = pd.json_normalize(kill, sep='_')
        temp_kills['matchid'] = matchid

        round_id = str(uuid.uuid5(uuid.NAMESPACE_OID, matchid + str(temp_kills['round'].values[0])))
        temp_kills['round_id'] = round_id

        kill_id = str(uuid.uuid5(uuid.NAMESPACE_OID, matchid + str(temp_kills['round'].values[0]) + temp_kills['killer_puuid'].values[0] + temp_kills['victim_puuid'].values[0]))

        temp_kills['kill_id'] = kill_id

        df_kills = pd.concat([df_kills, temp_kills])


        if len(assistants) >= 1:
            temp_assistants = pd.json_normalize(assistants)
            temp_assistants['kill_id'] = kill_id
            df_assistants = pd.concat([df_assistants, temp_assistants])


        temp_player_locations_on_kill = pd.json_normalize(player_locations_on_kill, sep='_')
        temp_player_locations_on_kill['kill_id'] = kill_id

        df_player_locations_on_kill = pd.concat([df_player_locations_on_kill, temp_player_locations_on_kill])

    df_kills.reset_index(inplace=True, drop=True)
    df_player_locations_on_kill.reset_index(inplace=True, drop=True)
    df_assistants.reset_index(inplace=True, drop=True)
    
    logging.info(f'Aplicando função de metadata')
    # Adjusting datatype
    df_metadata = reinforce_col_dtype(df_metadata, data)
    df_player = reinforce_col_dtype(df_player, data)
    df_player_ability_casts = reinforce_col_dtype(df_player_ability_casts, data)
    df_player_assets = reinforce_col_dtype(df_player_assets, data)
    df_player_behavior = reinforce_col_dtype(df_player_behavior, data)
    df_player_economy = reinforce_col_dtype(df_player_economy, data)
    df_player_platform = reinforce_col_dtype(df_player_platform, data)
    df_player_session_playtime = reinforce_col_dtype(df_player_session_playtime, data)
    df_player_stats = reinforce_col_dtype(df_player_stats, data)
    df_round_plant_events = reinforce_col_dtype(df_round_plant_events, data)
    df_player_locations_on_plant = reinforce_col_dtype(df_player_locations_on_plant, data)
    df_round_defuse_events = reinforce_col_dtype(df_round_defuse_events, data)
    df_player_locations_on_defuse = reinforce_col_dtype(df_player_locations_on_defuse, data)
    df_teams = reinforce_col_dtype(df_teams, data)
    df_round = reinforce_col_dtype(df_round, data)
    df_kills = reinforce_col_dtype(df_kills, data)
    df_player_locations_on_kill = reinforce_col_dtype(df_player_locations_on_kill, data)
    df_assistants = reinforce_col_dtype(df_assistants, data)
    df_round_player_stats = reinforce_col_dtype(df_round_player_stats, data)
    df_round_player_damage_events = reinforce_col_dtype(df_round_player_damage_events, data)
   
    dfs = [
        df_metadata,
        df_player,
        df_player_ability_casts,
        df_player_assets,
        df_player_behavior,
        df_player_economy,
        df_player_platform,
        df_player_session_playtime,
        df_player_stats,
        df_round_plant_events,
        df_player_locations_on_plant,
        df_round_defuse_events,
        df_player_locations_on_defuse,
        df_teams,
        df_round,
        df_kills,
        df_player_locations_on_kill,
        df_assistants,
        df_round_player_stats,
        df_round_player_damage_events
    ]
    
    
    logging.info(f'Iniciando a ingestão no BQ')
    print('Starting ingestion...')
    for df in dfs:
        
        table_name = retrieve_name(df)
        logging.info(f'Ingerindo o {table_name}')
        print(f'Table: {table_name}')
        table_name = table_name.replace('df_', '')
      
        df.to_gbq(
          f'tabular_valorant_match.{table_name}',
          'thales-1615931464192',
          if_exists='append',
          table_schema=build_schema(df)
        ) 
      
        print('Done \n')
    count += 1


# In[ ]:





# In[ ]:




