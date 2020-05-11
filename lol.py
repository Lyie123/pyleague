import requests
import pandas as pd
import re
import unicodedata
import logging                                      #https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
from time import sleep
from typing import List, Dict
from sqlalchemy import create_engine, exc
from sqlalchemy.sql import text
from sqlalchemy.types import VARCHAR

import logging


logging.basicConfig(filename='example.log', level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%d.%m.%Y %H:%M:%S')

'''
todos:
add forgein key support
add observe summoner
add delete observe summoner
'''

class LeagueApi:
    def __init__(self, api_key: str) -> None:
        self.key = api_key
        self.header = {
            'Origin': 'https://developer.riotgames.com',
            'X-Riot-Token': self.key
        }
        self.query_delay_time = 100

    def __snake_case__(self, camel_case: str) -> str:
        return re.sub(r'(?<!^)(?=[A-Z])', '_', camel_case).lower()

    def __post_query__(self, query: str) -> dict:
        sleep(0.2)
        r = requests.get(query, headers=self.header)
        if r.status_code == 403:
            logging.error('Forbidden request. Api key is not valid.')
            raise Exception('Forbidden request. Api key is not valid.')
        elif r.status_code == 429:
            logging.warning('Rate limit exceeded. Sleep 120 seconds.')
            sleep(120)
            try:
                r = requests.get(query, headers=self.header)
                return r.json()
            except:
                if r.status_code != 200:
                    logging.error('Repeated Error after rate limited exceeded and 120 seconds timeout')
                    raise Exception('Repeated Error after rate limited exceeded and 120 seconds timeout')
        else:
            return r.json()

    def get_summoner_by_name(self, name: str) -> pd.DataFrame:
        result = self.__post_query__('https://euw1.api.riotgames.com/lol/summoner/v4/summoners/by-name/'+name)
        df_result = pd.DataFrame([result])
        df_result.columns = map(self.__snake_case__, df_result.columns)
        df_result.rename({'id': 'summoner_id', 'name': 'summoner_name'}, axis=1, inplace=True)
        df_result.set_index('account_id', inplace=True)
        df_result['revision_date'] = pd.to_datetime(df_result['revision_date'], unit='ms')
        return df_result

    def get_summoner_by_id(self, id: str) -> pd.DataFrame:
        result = self.__post_query__('https://euw1.api.riotgames.com/lol/summoner/v4/summoners/by-account/'+str(id))
        df_result = pd.DataFrame([result])
        df_result.columns = map(self.__snake_case__, df_result.columns)
        df_result.rename({'id': 'summoner_id', 'name': 'summoner_name'}, axis=1, inplace=True)
        df_result.set_index('account_id', inplace=True)
        df_result['revision_date'] = pd.to_datetime(df_result['revision_date'], unit='ms')
        return df_result

    def get_match_list(self, account_id: str) -> pd.DataFrame:
        result = self.__post_query__('https://euw1.api.riotgames.com/lol/match/v4/matchlists/by-account/'+account_id)['matches']
        df_matches = pd.DataFrame(result)
        df_matches.columns = map(self.__snake_case__, df_matches.columns)
        return df_matches

    def get_match_details(self, game_id: str) -> Dict[str, pd.DataFrame]:
        result = self.__post_query__('https://euw1.api.riotgames.com/lol/match/v4/matches/'+game_id)
        df_match_details = pd.DataFrame([result])
        df_match_details['gameCreation'] = pd.to_datetime(df_match_details['gameCreation'], unit='ms')
        df_teams = pd.DataFrame(df_match_details['teams'][0])

        try:
            df_bans_team_100 = pd.DataFrame(df_teams['bans'][0])
            df_bans_team_100['teamId'] = 100
            df_bans_team_100['gameId'] = df_match_details['gameId'].max()
            df_bans_team_200 = pd.DataFrame(df_teams['bans'][1])
            df_bans_team_200['teamId'] = 200
            df_bans_team_200['gameId'] = df_match_details['gameId'].max()
            df_bans = pd.concat([df_bans_team_100, df_bans_team_200])
            df_bans.columns = map(self.__snake_case__, df_bans.columns)
            df_bans = df_bans.set_index(['game_id', 'team_id', 'pick_turn'])
        except Exception as e:
            df_bans = pd.DataFrame()
            logging.warning('Error while parsing bans of participants. Exception: %s', e)

        df_participants = pd.DataFrame(df_match_details['participants'][0])
        df_participants['gameId'] = df_match_details['gameId'].max()
        df_participants_stats = pd.DataFrame(list(df_participants.stats))
        df_key_table = df_participants[['gameId', 'teamId', 'participantId']]
        df_participants_stats = pd.merge(df_key_table, df_participants_stats, on=['participantId'])

        df_participants_timeline = pd.DataFrame(list(df_participants.timeline))

        df_participants = pd.merge(df_participants, df_participants_timeline[['participantId', 'role', 'lane']], on=['participantId'])

        try:
            df_participants_timeline_creeps_per_min_deltas = pd.DataFrame(list(df_participants_timeline.csDiffPerMinDeltas.dropna()))
            df_participants_timeline_xp_per_min_deltas = pd.DataFrame(list(df_participants_timeline.xpPerMinDeltas.dropna()))
            df_participants_timeline_gold_per_min_deltas = pd.DataFrame(list(df_participants_timeline.goldPerMinDeltas.dropna()))
            df_participants_timeline_cs_diff_per_min_deltas = pd.DataFrame(list(df_participants_timeline.csDiffPerMinDeltas.dropna()))
            df_participants_timeline_xp_diff_per_min_deltas = pd.DataFrame(list(df_participants_timeline.xpDiffPerMinDeltas.dropna()))
            df_participants_timeline_damage_taken_per_min_deltas = pd.DataFrame(list(df_participants_timeline.damageTakenPerMinDeltas.dropna()))
            df_participants_timeline_damage_taken_diff_per_min_deltas = pd.DataFrame(list(df_participants_timeline.damageTakenDiffPerMinDeltas.dropna()))

            df_timeline = [
            df_participants_timeline_creeps_per_min_deltas,
            df_participants_timeline_xp_per_min_deltas,
            df_participants_timeline_gold_per_min_deltas,
            df_participants_timeline_cs_diff_per_min_deltas,
            df_participants_timeline_xp_diff_per_min_deltas,
            df_participants_timeline_damage_taken_per_min_deltas,
            df_participants_timeline_damage_taken_diff_per_min_deltas]

            for i in range(len(df_timeline)):
                df_timeline[i] = df_timeline[i].join(df_participants_timeline['participantId'])
                df_timeline[i] = pd.merge(df_timeline[i], df_key_table, on=['participantId'])
                df_timeline[i].columns = map(self.__snake_case__, df_timeline[i])
                df_timeline[i] = df_timeline[i].set_index(['game_id', 'team_id', 'participant_id'])

            # to fix. aram nicht alle timelines vorhanden. exception bei 4, 5, 6
            df_participants_timeline_creeps_per_min_deltas = df_timeline[0]
            df_participants_timeline_xp_per_min_deltas = df_timeline[1]
            df_participants_timeline_gold_per_min_deltas = df_timeline[2]
            df_participants_timeline_cs_diff_per_min_deltas = df_timeline[3]
            df_participants_timeline_xp_diff_per_min_deltas = df_timeline[4]
            df_participants_timeline_damage_taken_per_min_deltas = df_timeline[5]
            df_participants_timeline_damage_taken_diff_per_min_deltas = df_timeline[6]
        except AttributeError:
            df_timeline = []
            df_participants_timeline_creeps_per_min_deltas = pd.DataFrame()
            df_participants_timeline_xp_per_min_deltas = pd.DataFrame()
            df_participants_timeline_gold_per_min_deltas = pd.DataFrame()
            df_participants_timeline_cs_diff_per_min_deltas = pd.DataFrame()
            df_participants_timeline_xp_diff_per_min_deltas = pd.DataFrame()
            df_participants_timeline_damage_taken_per_min_deltas = pd.DataFrame()
            df_participants_timeline_damage_taken_diff_per_min_deltas = pd.DataFrame()
            print('warning: attribute_error. keine daten in timeline vorhanden. game_id: {0}'.format(game_id))




        #Tabelle participants
        df_participants_identities = pd.DataFrame(df_match_details['participantIdentities'][0])
        df_participants_identities = df_participants_identities.join(pd.DataFrame(list(df_participants_identities.player)))
        df_participants_identities = pd.merge(df_participants_identities, df_key_table, on=['participantId'])

        df_match_details.columns = map(self.__snake_case__, df_match_details.columns)
        df_teams.columns = map(self.__snake_case__, df_teams.columns)
        df_participants.columns = map(self.__snake_case__, df_participants.columns)
        df_participants_stats.columns = map(self.__snake_case__, df_participants_stats.columns)
        df_participants_timeline.columns = map(self.__snake_case__, df_participants_timeline.columns)
        df_participants_identities.columns = map(self.__snake_case__, df_participants_identities.columns)

        df_teams['game_id'] = df_match_details['game_id'].max()
        df_participants_identities = df_participants_identities.drop(columns=['player'])


        df_match_details.drop(columns=['teams', 'participants', 'participant_identities'], inplace=True)
        df_participants.drop(columns=['stats', 'timeline'], inplace=True)
        df_teams.drop(columns=['bans'], inplace=True)

        df_match_details = df_match_details.set_index(['game_id'])
        df_teams = df_teams.set_index(['game_id', 'team_id'])
        df_participants = df_participants.set_index(['game_id', 'team_id', 'participant_id'])
        df_participants_stats = df_participants_stats.set_index(['game_id', 'team_id', 'participant_id'])
        df_participants_identities = df_participants_identities.set_index(['game_id', 'team_id', 'participant_id'])
        df_participants_identities['summoner_name'] = df_participants_identities['summoner_name'].apply(lambda val: unicodedata.normalize('NFKD', val).encode('ascii', 'ignore').decode())

        df_result = {
            'matches': df_match_details,
            'teams': df_teams,
            'bans': df_bans,
            'participants': df_participants,
            'participants_identities': df_participants_identities,
            'participants_stats': df_participants_stats,
            'timeline_creeps_per_min': df_participants_timeline_creeps_per_min_deltas,
            'timeline_xp_per_min': df_participants_timeline_xp_per_min_deltas,
            'timeline_gold_per_min': df_participants_timeline_gold_per_min_deltas,
            'timeline_cs_diff_per_min': df_participants_timeline_cs_diff_per_min_deltas,
            'timeline_xp_diff_per_min': df_participants_timeline_xp_diff_per_min_deltas,
            'timeline_damage_taken_per_min': df_participants_timeline_damage_taken_per_min_deltas,
            'timeline_damage_taken_diff_per_min': df_participants_timeline_damage_taken_diff_per_min_deltas
        }
        return df_result


class SqlClient:
    def __init__(self, connection_string: str) -> None:
        self.engine = create_engine(connection_string)

    def create_table(self, table_name: str, data_frame: pd.DataFrame, use_index_as_pk: bool=True):
        data_frame.to_sql(table_name, con=self.engine, if_exists='replace', method='multi', dtype={c: VARCHAR(100) for c in data_frame.select_dtypes(include='object').index.names})
        if use_index_as_pk:
            query = 'ALTER TABLE ' + table_name + ' ADD PRIMARY KEY(' + ', '.join(data_frame.index.names) + ')'
            self.engine.execute(text(query))

    def merge_to_table(self, table_name: str, data_frame: pd.DataFrame):
        try:
            data_frame.to_sql(table_name, con=self.engine, if_exists='append', dtype={c: VARCHAR(100) for c in data_frame.select_dtypes(include='object').index.names})
        except exc.IntegrityError as e:
            if e.orig.args[0] == 1062:
                pass
            else:
                raise e

    def update_table(self, table_name: str, data_frame: pd.DataFrame) -> None:
        try:
            sql = 'UPDATE ' + table_name + ' set ' + ' and '.join(data_frame.columns + ' = ' + ['\''+str(e)+'\'' for e in data_frame.iloc[0].values]) + ' where ' + ' and '.join(data_frame.index + ' = ' + ['\''+str(e)+'\'' for e in data_frame.index.values])
            self.engine.execute(text(sql))
        except:
            self.merge_to_table(table_name, data_frame)

    def read_table(self, sql_query: str) -> pd.DataFrame:
        return pd.read_sql_query(sql_query, self.engine)

    def insert_data_frame(self, table_name: str, data_frame: pd.DataFrame):
        table_exists = self.engine.has_table(table_name)
        if not table_exists:
            self.create_table(table_name, data_frame)
        else:
            self.merge_to_table(table_name, data_frame)

class WebApi:
    def __init__(self, api_key: str, connection_string : str) -> None:
        self.league_api = LeagueApi(api_key)
        self.sql_client = SqlClient(connection_string)

    def update_summoner(self, summoner_name: str) -> None:
        logging.debug('Update summoner "%s"', summoner_name)

        df_summoner = self.league_api.get_summoner_by_name(summoner_name)
        self.sql_client.update_table('summoner', df_summoner)

        sql = 'SELECT * FROM v_summoner_matches WHERE account_id' + ' = \'' + df_summoner.index[0] + '\''
        df_matches_already_loaded = self.sql_client.read_table(sql)

        df_match_list = self.league_api.get_match_list(df_summoner.index[0])
        df_new_matches = df_match_list[~df_match_list['game_id'].isin(df_matches_already_loaded['game_id'])]

        logging.debug('Fetch %s out of %s matches', df_new_matches.game_id.size, df_match_list.game_id.size)

        for m in df_new_matches.game_id:
            df_details = self.league_api.get_match_details(str(m))
            for name, table in df_details.items():
                self.sql_client.insert_data_frame(name, table)
            logging.debug('Merge match "%s" into table', m)

    def update_observed_summoner(self):
        sql = 'SELECT * FROM observed_summoner'
        df_observed = self.sql_client.read_table(sql)
        for index, row in df_observed.iterrows():
            summoner_name = self.league_api.get_summoner_by_id(row['account_id']).loc[row['account_id'], 'summoner_name']
            logging.debug('Update observed summoner "%s"', summoner_name)
            self.update_summoner(summoner_name)

