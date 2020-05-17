import requests
import pandas as pd
import re
import unicodedata
import json
import logging                                  #https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
from datetime import datetime
from typing import Dict, List
from time import sleep
from typing import List, Dict
from sqlalchemy import create_engine, exc
from sqlalchemy.sql import text
from sqlalchemy.types import VARCHAR
from sqlalchemy.orm.exc import NoResultFound

logging.basicConfig(filename='league_api.log', level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%d.%m.%Y %H:%M:%S')


class SqlEngine:
    def __init__(self, connection_string: str) -> None:
        self.engine = create_engine(connection_string)
    def create_table(self, table_name: str, table: pd.DataFrame) -> None:
        """
        Erstellt eine neue Tabelle.
        Wenn Tabelle bereits existiert, wird sie ersetzt.
        Index wird als Primary Key benutzt.

        Arguments:
            table_name {str} -- Name der Tabelle die erstellt werden soll
            table {pd.DataFrame} -- Tabelle die erstellt werden soll
        """
        
        table.to_sql(table_name, con=self.engine, if_exists='replace', method='multi', dtype={c: VARCHAR(100) for c in table.select_dtypes(include='object').index.names})
        sql = 'ALTER TABLE ' + table_name + ' ADD PRIMARY KEY(' + ', '.join(table.index.names) + ')'
        self.engine.execute(text(sql))
    def merge_table(self, table_name: str, table: pd.DataFrame) -> None:
        table_exists = self.engine.has_table(table_name)
        if not table_exists:
            self.create_table(table_name, table)
        else:
            try:
                table.to_sql(table_name, con=self.engine, if_exists='append', dtype={c: VARCHAR(100) for c in table.select_dtypes(include='object').index.names})
            except exc.IntegrityError as ex:
                if ex.orig.args[0] == 1062:
                    pass
                else:
                    raise ex
    def table_exists(self, table_name: str) -> bool:
        return self.engine.has_table(table_name)
    def update_table(self, table_name: str, table: pd.DataFrame) -> None:
        try:
            sql = 'update ' + table_name + ' set ' + ' and '.join(table.columns + ' = ' + ['\''+str(e)+'\'' for e in table.iloc[0].values]) + ' where ' + ' and '.join(table.index + ' = ' + ['\''+str(e)+'\'' for e in table.index.values])
            self.engine.execute(text(sql))
        except:
            self.merge_table(table_name, table)
    def execute(self, query: str):
        return self.engine.execute(query)

class StaticContent:
    @staticmethod
    def load_champion_json(engine: SqlEngine) -> None:    
        f = open('/home/lyie/Desktop/projects/pyleague/data/9.3.1/data/en_US/champion.json')
        content = json.loads(f.read())
        frame = pd.DataFrame()
        for key, value in content['data'].items():
            frame = frame.append(pd.json_normalize(value), ignore_index=True)
        frame.columns = list(map(lambda x: re.sub('\.', '_', x), frame.columns))
        frame.rename(columns={'key': 'champion_id'}, inplace=True)
        frame.drop(columns=['id'], inplace=True)
        frame.set_index('champion_id', inplace=True)
        frame['tags'] = frame.tags.apply(lambda x: ', '.join(x))

        engine.create_table('champions', frame)

class RiotApi:
    def __init__(self, api_key: str) -> None:
        self.key = api_key
        self.header = {
            'Origin': 'https://developer.riotgames.com',
            'X-Riot-Token': self.key
        }
        self.query_delay_time = 100
    def __snake_case(self, camel_case: str) -> str:
        return re.sub(r'(?<!^)(?=[A-Z])', '_', camel_case).lower()
    def __post_query(self, query: str) -> dict:
        sleep(0.2)
        r = requests.get(query, headers=self.header)
        if r.status_code == 403:
            logging.error('Forbidden request. Api key is not valid.')
            raise Exception('Forbidden request. Api key is not valid.')
        elif r.status_code == 404:
            # no data found
            raise NoResultFound('No results from request.')
        elif r.status_code == 429:
            logging.warning('Rate limit exceeded. Sleep 121 seconds.')
            sleep(121)
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
        result = self.__post_query('https://euw1.api.riotgames.com/lol/summoner/v4/summoners/by-name/'+name)
        df_result = pd.json_normalize(result)
        df_result.columns = map(self.__snake_case, df_result.columns)
        df_result.rename({'id': 'summoner_id', 'name': 'summoner_name'}, axis=1, inplace=True)
        df_result.set_index('account_id', inplace=True)
        df_result['revision_date'] = pd.to_datetime(df_result['revision_date'], unit='ms')
        return df_result
    def get_match_list(self, account_id: str, begin_time: str = '0') -> pd.DataFrame:
        try:
            result = self.__post_query('https://euw1.api.riotgames.com/lol/match/v4/matchlists/by-account/'+account_id+'?beginTime='+begin_time)['matches']
            df_matches = pd.json_normalize(result)
            df_matches.columns = map(self.__snake_case, df_matches.columns)
            return df_matches
        except NoResultFound:
            return pd.DataFrame()
    def get_match_details(self, match_id: str) -> Dict[str, pd.DataFrame]:
        result = self.__post_query('https://euw1.api.riotgames.com/lol/match/v4/matches/'+str(match_id))
        frames = {}
        frames.update(self.__extract_match_data(result))
        frames.update(self.__extract_teams_data(result))
        frames.update(self.__extract_bans_data(result))
        frames.update(self.__extract_participants_data(result))
        frames.update(self.__extract_stats_data(result))
        return frames

    def __extract_match_data(self, data: json) -> Dict[str, pd.DataFrame]:
        game = pd.json_normalize(data)
        game['gameCreation'] = pd.to_datetime(game['gameCreation'], unit='ms')
        game = game.drop(columns=['teams', 'participants', 'participantIdentities'])
        game.columns = map(self.__snake_case, game.columns)
        game = game.set_index('game_id')
        return {'matches': game}
    def __extract_teams_data(self, data: json) -> Dict[str, pd.DataFrame]:
        teams = pd.json_normalize(data, record_path=['teams'], meta='gameId', sep='_', max_level=0)
        teams = teams.drop(columns=['bans'])
        teams.columns = map(self.__snake_case, teams.columns)
        teams = teams.set_index(['game_id', 'team_id'])
        return {'teams': teams}
    def __extract_bans_data(self, data: json) -> Dict[str, pd.DataFrame]:
        bans = pd.json_normalize(data, record_path=['teams', 'bans'], meta=['gameId', ['teams', 'teamId']])
        bans = bans.rename(columns={'teams.teamId': 'teamId'})
        bans.columns = map(self.__snake_case, bans.columns)
        bans = bans.set_index(['game_id', 'team_id', 'pick_turn'])
        return {'bans': bans}
    def __extract_participants_data(self, data: json) -> Dict[str, pd.DataFrame]:
        participants = pd.json_normalize(data, record_path=['participantIdentities'], meta=['gameId'])
        participants.columns = map(lambda x: re.sub('player.', '', x), participants.columns)
        participants.columns = map(self.__snake_case, participants.columns)
        participants = participants.set_index(['game_id', 'participant_id'])
        participants['summoner_name'] = participants['summoner_name'].apply(lambda val: unicodedata.normalize('NFKD', val).encode('ascii', 'ignore').decode())
        return {'participants': participants}
    def __extract_stats_data(self, data: json) -> Dict[str, pd.DataFrame]:
        stats = pd.json_normalize(data, record_path=['participants'], meta=['gameId'])
        stats = stats.drop(columns=list(stats.filter(regex='timeline')))
        stats = stats.drop(columns=['participantId'])
        stats.columns = map(lambda x: re.sub('stats.', '', x), stats.columns)
        stats.columns = map(self.__snake_case, stats.columns)
        stats = stats.set_index(['game_id', 'team_id', 'participant_id'])
        return {'stats': stats}

class Controller:
    def __init__(self, api_key: str, connection_string: str):
        self.api = RiotApi(api_key)
        self.engine = SqlEngine(connection_string)
    def update_summoner(self, summoner_name: str) -> None:
        """
        Update Summary profile with specific name.
        1. fetch summary of summoner by name
        2. fetch all matches > last update of summoner profile
        3. merge matches into database

        Arguments:
            summoner_name {str} -- name of summoner who gets updated
        """
        logging.debug('Update summoner "{0}"'.format(summoner_name))

        summoner = self.api.get_summoner_by_name(summoner_name)
        self.engine.update_table('summoner', summoner)

        query = 'SELECT GET_SUMMONER_LAST_UPDATE("{0}")'.format(summoner.index[0]);
        last_update = self.engine.execute(query).first()[0]
        last_update = str(int(last_update.timestamp()*1000))
        #self.engine.merge_table('summoner', summoner)

        matches = self.api.get_match_list(summoner.index[0], last_update)
        amount_of_matches = matches.shape[0]
        logging.debug('Fetched %s matches', amount_of_matches)

        if amount_of_matches == 0:
            query = 'CALL UPDATE_SUMMONER_LAST_UPDATE("{0}")'.format(summoner.index[0])
            self.engine.execute(query)
            return

        for m in matches.game_id:
            details = self.api.get_match_details(str(m))
            for name, table in details.items():
                self.engine.merge_table(name, table)
            logging.debug('Merge match "{0}" into table'.format(m))
        query = 'CALL UPDATE_SUMMONER_LAST_UPDATE("{0}")'.format(summoner.index[0])
        self.engine.execute(query)