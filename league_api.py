import requests
import pandas as pd
from datetime import datetime
import re
# import unicodedata
import json
import logging                                  # https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
# from datetime import datetime
from typing import Dict
from time import sleep
from sqlalchemy import create_engine, exc
from sqlalchemy.sql import text
from sqlalchemy.types import VARCHAR
from sqlalchemy.orm.exc import NoResultFound
from enum import Enum

logging.basicConfig(filename='league_api.log', level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%d.%m.%Y %H:%M:%S')

class QueueType(Enum):
    RANKED_SOLO = 'RANKED_SOLO_5x5'
    RANKED_FLEX = 'RANKED_FLEX_SR'

class RiotApi:
    def __init__(self, api_key: str) -> None:
        self.key = api_key
        self.header = {
            'Origin': 'https://developer.riotgames.com',
            'X-Riot-Token': self.key
        }
        self.query_delay_time = 100
        self.version = '10.23.1'

    def __snake_case(self, camel_case: str) -> str:
        return re.sub(r'(?<!^)(?=[A-Z])', '_', camel_case).lower()

    def __post_query(self, query: str, is_first_run: bool=True) -> dict:
        sleep(0.5)
        r = requests.get(query, headers=self.header)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 403:
            logging.error('Forbidden request. Api key is not valid.')
            raise Exception('Forbidden request. Api key is not valid.')
        elif r.status_code == 504:
            sleep(5)
            try:
                r = requests.get(query, headers=self.header)
                return r.json()
            except Exception:
                logging.error('Gateway timeout')
                raise Exception('Gateway timeout...')
        elif r.status_code == 404:
            # no data found
            raise NoResultFound('No results from request.')
        elif r.status_code == 429 and is_first_run:
            logging.warning('Rate limit exceeded. Sleep 121 seconds.')
            sleep(121)
            try:
                r = requests.get(query, headers=self.header, is_first_run=False)
                return r.json()
            except Exception:
                if r.status_code != 200:
                    logging.error('Repeated Error after rate limited exceeded and 120 seconds timeout')
                    raise Exception('Repeated Error after rate limited exceeded and 120 seconds timeout')
        else:
            raise Exception('code {0} error on requesting {1}'.format(r.status_code, query))

    def get_summoner_by_name(self, name: str) -> pd.DataFrame:
        result = self.__post_query('https://euw1.api.riotgames.com/lol/summoner/v4/summoners/by-name/'+name)
        df_result = pd.json_normalize(result)
        df_result.columns = map(self.__snake_case, df_result.columns)
        df_result.rename({'id': 'summoner_id', 'name': 'summoner_name'}, axis=1, inplace=True)
        df_result.set_index('account_id', inplace=True)
        df_result['revision_date'] = pd.to_datetime(df_result['revision_date'], unit='ms')
        return df_result

    def get_match_list(self, account_id: str, end_index: int=100, start_index: int=0, queue_id: int=-1, champion_id: int=-1, full: bool=False, begin_time: datetime=None) -> pd.DataFrame:
        query = 'https://euw1.api.riotgames.com/lol/match/v4/matchlists/by-account/'+account_id+'?beginIndex='+str(start_index)+'&endIndex='+str(end_index)
        if begin_time is not None:
            stamp = round(begin_time.timestamp()*1000)
            query += '&beginTime={0}'.format(str(stamp))
        if champion_id != -1:
            query += '&champion='+str(champion_id)
        if queue_id != -1:
            query += '&queue='+str(queue_id)
        result = self.__post_query(query)['matches']
        df_matches = pd.json_normalize(result)
        df_matches.columns = map(self.__snake_case, df_matches.columns)

        if full and len(df_matches) > 0:
            df_matches = pd.concat([df_matches, self.get_match_list(account_id, start_index=end_index, end_index=end_index+100, queue_id=queue_id, champion_id=champion_id, full=full)])

        return df_matches

    def get_match_details(self, match_id: str) -> Dict[str, pd.DataFrame]:
        result = self.__post_query('https://euw1.api.riotgames.com/lol/match/v4/matches/'+str(match_id))
        frames = {}
        frames.update(self.__extract_match_data(result))
        frames.update(self.__extract_teams_data(result))
        frames.update(self.__extract_bans_data(result))
        frames.update(self.__extract_participants_data(result))
        frames.update(self.__extract_stats_data(result))
        return frames

    def get_timeline(self, match_id: str) -> Dict[str, pd.DataFrame]:
        query = 'https://euw1.api.riotgames.com/lol/match/v4/timelines/by-match/'+str(match_id)
        result = self.__post_query(query)

        df_participants = pd.DataFrame()
        df_events = pd.DataFrame()
        for frame in result['frames']:
            for participant in frame['participantFrames'].values():
                buffer = pd.json_normalize(participant, sep='_')
                buffer['timestamp'] = frame['timestamp']
                df_participants = df_participants.append(buffer, ignore_index=True)
            for event in frame['events']:
                buffer = pd.json_normalize(event, sep='_')
                df_events = df_events.append(buffer, ignore_index=True)

        df_participants.columns = map(self.__snake_case, df_participants.columns)
        df_participants['game_id'] = str(match_id)
        df_participants = df_participants.set_index(['game_id', 'timestamp', 'participant_id'])

        df_events.columns =  map(self.__snake_case, df_events.columns)
        df_events.participant_id = df_events.participant_id.fillna('0')
        df_events['game_id'] = str(match_id)
        df_events['sequence'] = df_events.groupby(['game_id', 'timestamp', 'participant_id', 'type']).cumcount()
        df_events = df_events.set_index(['game_id', 'timestamp', 'participant_id', 'type'])
        
        frames = {}
        frames.update({'timeline_participants': df_participants})
        frames.update({'timeline_events': df_events})

        return frames

    def get_leaderboard(self, queue_type: QueueType) -> pd.DataFrame:
        query = 'https://euw1.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{0}'.format(queue_type.value)
        result = self.__post_query(query)
        result = pd.json_normalize(result, record_path='entries')
        result.columns = map(self.__snake_case, result.columns)
        result.set_index('summoner_id', inplace=True)
        return result

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
        try:
            bans = pd.json_normalize(data, record_path=['teams', 'bans'], meta=['gameId', ['teams', 'teamId']])
            bans = bans.rename(columns={'teams.teamId': 'teamId'})
            bans.columns = map(self.__snake_case, bans.columns)
            bans = bans.set_index(['game_id', 'team_id', 'pick_turn'])
            return {'bans': bans}
        except KeyError:
            return {'bans': pd.DataFrame()}

    def __extract_participants_data(self, data: json) -> Dict[str, pd.DataFrame]:
        participants = pd.json_normalize(data, record_path=['participantIdentities'], meta=['gameId'])
        participants.columns = map(lambda x: re.sub('player.', '', x), participants.columns)
        participants.columns = map(self.__snake_case, participants.columns)
        participants = participants.set_index(['game_id', 'participant_id'])
        #participants['summoner_name'] = participants['summoner_name'].apply(lambda val: unicodedata.normalize('NFKD', val).encode('ascii', 'ignore').decode())
        return {'participants': participants}

    def __extract_stats_data(self, data: json) -> Dict[str, pd.DataFrame]:
        stats = pd.json_normalize(data, record_path=['participants'], meta=['gameId'])
        stats = stats.rename(columns={'timeline.lane': 'lane', 'timeline.role': 'role'})
        stats = stats.drop(columns=list(stats.filter(regex='timeline')))
        stats = stats.drop(columns=['participantId'])
        stats.columns = map(lambda x: re.sub('stats.', '', x), stats.columns)
        stats.columns = map(self.__snake_case, stats.columns)
        stats = stats.set_index(['game_id', 'team_id', 'participant_id'])
        return {'stats': stats}
    
    def get_queue_types(self) -> pd.DataFrame:
        df_result = pd.read_json('http://static.developer.riotgames.com/docs/lol/queues.json')
        df_result.columns = map(self.__snake_case, df_result.columns)
        df_result = df_result.set_index('queue_id')
        return df_result

    def get_champion_json(self) -> pd.DataFrame:
        content = requests.get('http://ddragon.leagueoflegends.com/cdn/{0}/data/en_US/champion.json'.format(self.version)).json()
        table = pd.DataFrame()
        for value in content['data'].values():
            table = table.append(pd.json_normalize(value, sep='_'), ignore_index=True)
        table.columns = map(self.__snake_case, table)
        table.columns = table.columns.str.replace('stats_', '')
        table.rename(columns={'key': 'champion_id'}, inplace=True)
        table.drop(columns=['id'], inplace=True)
        table.set_index('champion_id', inplace=True)
        table['tags'] = table.tags.apply(lambda x: ', '.join(x))
        return table
