import json
import uuid
import inspect

import pandas as pd
from google.cloud import storage, bigquery
#pd.set_option('display.max_columns', 500)

def build_schema(df):    

    temp_schema = pd.DataFrame(df.dtypes, columns=['dtype']).reset_index()

    schema_list = []

    for i, row in temp_schema.iterrows():
        if row['dtype'] == 'int64':
            dtype = 'INTEGER'
        elif row['dtype'] == 'float64':
            dtype = 'FLOAT'
        elif row['dtype'] == 'object':
            dtype = 'STRING'
        elif row['dtype'] == 'bool':
            dtype = 'BOOLEAN'

        schema_list.append({'name': row['index'], 'type': f'{dtype}'})

    return schema_list



def retrieve_name(var):
    callers_local_vars = inspect.currentframe().f_back.f_locals.items()
    return [var_name for var_name, var_val in callers_local_vars if var_val is var][0]

path = 'credentials/service_account.json'
client = storage.Client.from_service_account_json(path)

bucket_name = "valorant_data"

bucket = client.bucket(bucket_name)

client_gbq = bigquery.Client.from_service_account_json(path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(path)

# Get all stored matchs
folder_name = "match"

folder = bucket.blob(folder_name)

matches = []

for blob in bucket.list_blobs():
    matches.append(blob.name.replace('match/', '').replace('.json', ''))

#matchid = '0d9063f2-4b7f-4737-8244-4c5e32924dfa'

#matches = [matchid]

for matchid in matches:

    with open(f'{matchid}.json', 'r') as f:
    match = json.load(f)

    #metadata = match.pop('metadata')
    #players = match.pop('players')
    #teams = match.pop('teams')
    #rounds = match.pop('rounds')
    #kills = match.pop('kills')
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


    for player in all_players:

        puuid = player['puuid']

        for key in json_players_keys:

            exec(f"player_{key} = player.pop('{key}')")
            #exec(f"player_{key} = player['{key}']")
            

        temp_player = pd.json_normalize(player)
        temp_player['matchid'] = matchid
        df_player = pd.concat([df_player, temp_player])

        temp_player_ability_casts = pd.json_normalize(player_ability_casts)
        temp_player_ability_casts['matchid'] = matchid
        temp_player_ability_casts['puuid'] = puuid
        df_player_ability_casts = pd.concat([df_player_ability_casts, temp_player_ability_casts])

        temp_player_assets = pd.json_normalize(player_assets, sep='_')
        temp_player_assets['matchid'] = matchid
        temp_player_assets['puuid'] = puuid
        df_player_assets = pd.concat([df_player_assets, temp_player_assets])

        temp_player_behavior = pd.json_normalize(player_behavior, sep='_')
        temp_player_behavior['matchid'] = matchid
        temp_player_behavior['puuid'] = puuid
        df_player_behavior = pd.concat([df_player_behavior, temp_player_behavior])

        temp_player_economy = pd.json_normalize(player_economy, sep='_')
        temp_player_economy['matchid'] = matchid
        temp_player_economy['puuid'] = puuid
        df_player_economy = pd.concat([df_player_economy, temp_player_economy])

        temp_player_platform = pd.json_normalize(player_platform, sep='_')
        temp_player_platform['matchid'] = matchid
        temp_player_platform['puuid'] = puuid
        df_player_platform = pd.concat([df_player_platform, temp_player_platform])

        temp_player_session_playtime = pd.json_normalize(player_session_playtime, sep='_')
        temp_player_session_playtime['matchid'] = matchid
        temp_player_session_playtime['puuid'] = puuid
        df_player_session_playtime = pd.concat([df_player_session_playtime, temp_player_session_playtime])

        temp_player_stats = pd.json_normalize(player_stats, sep='_')
        temp_player_stats['matchid'] = matchid
        temp_player_stats['puuid'] = puuid
        df_player_stats = pd.concat([df_player_stats, temp_player_stats])

    df_player.reset_index(inplace=True, drop=True)
    df_player_ability_casts.reset_index(inplace=True, drop=True)
    df_player_assets.reset_index(inplace=True, drop=True)
    df_player_behavior.reset_index(inplace=True, drop=True)
    df_player_economy.reset_index(inplace=True, drop=True)
    df_player_platform.reset_index(inplace=True, drop=True)
    df_player_session_playtime.reset_index(inplace=True, drop=True)
    df_player_stats.reset_index(inplace=True, drop=True)

    ## Teams
    df_teams = pd.json_normalize(teams, sep='_')
    df_teams['matchid'] = matchid

    ## Rounds
    json_round_keys = ['plant_events', 'defuse_events', 'player_stats']

    round_number = 0
    df_round_plant_events = pd.DataFrame()
    df_player_locations_on_plant = pd.DataFrame()
    df_round_defuse_events = pd.DataFrame()
    df_player_locations_on_defuse = pd.DataFrame()
    
    df_round = pd.DataFrame()

    for rnd in rounds:

        for key in json_round_keys:
            exec(f"round_{key} = rnd.pop('{key}')")
            #exec(f"round_{key} = rnd['{key}']")
            

        round_id = str(uuid.uuid5(uuid.NAMESPACE_OID, matchid + str(round_number)))

        # Round
        temp_round = pd.DataFrame([rnd])
        temp_round['round_id'] = round_id

        df_round = pd.concat([df_round, temp_round])

        # Plant Events

        player_locations_on_plant = round_plant_events.pop('player_locations_on_plant')
        #player_locations_on_plant = round_plant_events['player_locations_on_plant']

        temp_round_plant_events = pd.json_normalize(round_plant_events, sep='_')

        temp_round_plant_events['round_id'] = round_id

        df_round_plant_events = pd.concat([df_round_plant_events, temp_round_plant_events])

        if player_locations_on_plant is not None:

            temp_player_locations_on_plant = pd.json_normalize(player_locations_on_plant, sep='_')

            temp_player_locations_on_plant['round_id'] = round_id

            df_player_locations_on_plant = pd.concat([df_player_locations_on_plant, temp_player_locations_on_plant])

        # Defuse Events

        player_locations_on_defuse = round_defuse_events.pop('player_locations_on_defuse')
        #player_locations_on_defuse = round_defuse_events['player_locations_on_defuse']

        temp_round_defuse_events = pd.json_normalize(round_defuse_events, sep='_')

        temp_round_defuse_events['round_id'] = round_id

        df_round_defuse_events = pd.concat([df_round_defuse_events, temp_round_defuse_events])


        if player_locations_on_defuse is not None:

            temp_player_locations_on_defuse = pd.json_normalize(player_locations_on_defuse, sep='_')

            temp_player_locations_on_defuse['round_id'] = round_id

            df_player_locations_on_defuse = pd.concat([df_player_locations_on_defuse, temp_player_locations_on_defuse])


        round_number += 1

    ### Kills

    df_kills = pd.DataFrame()
    df_player_locations_on_kill = pd.DataFrame()
    df_assistants = pd.DataFrame()

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
        df_assistants
    ]
    
    
    for df in dfs:
        df.to_gbq(
            f'tabular_valorant_match.{retrieve_name(df).replace('df_', '')}',
            'thales-1615931464192',
            if_exists='append',
            table_schema=build_schema(df)
        ) 
    
