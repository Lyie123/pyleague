from datetime import datetime
import pandas as pd
from sqlalchemy.orm.exc import NoResultFound
import league_api as api
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from psycopg2.extensions import register_adapter, AsIs
from numpy import int64


# conversation numpy.int64 to int
register_adapter(int64, AsIs)

Base = declarative_base()

class Summoner(Base):
    __tablename__ = 'summoner'

    account_id = Column(String, primary_key=True)

    summoner_id = Column(String)
    puuid = Column(String)
    summoner_name = Column(String)
    profile_icon_id = Column(Integer)
    revision_date = Column(DateTime)
    summoner_level = Column(Integer)

class Match(Base):
    __tablename__ = 'matches'

    game_id = Column(String, primary_key=True)

    platform_id = Column(String)
    game_creation = Column(DateTime)
    game_duration = Column(Integer)
    queue_id = Column(String)
    map_id = Column(String)
    season_id = Column(String)
    game_version = Column(String)
    game_mode = Column(String)
    game_type = Column(String)

class Team(Base):
    __tablename__ = 'teams'

    game_id = Column(String, ForeignKey('matches.game_id', ondelete='CASCADE'), primary_key=True)
    team_id = Column(String, primary_key=True)

    win = Column(String)
    first_blood = Column(Boolean)
    first_tower = Column(Boolean)
    first_inhibitor = Column(Boolean)
    first_baron = Column(Boolean)
    first_dragon = Column(Boolean)
    first_rift_herald = Column(Boolean)
    tower_kills = Column(Integer)
    inhibitor_kills = Column(Integer)
    baron_kills = Column(Integer)
    dragon_kills = Column(Integer)
    vilemaw_kills = Column(Integer)
    rift_herald_kills = Column(Integer)
    dominion_victory_score = Column(Integer)

class Ban(Base):
    __tablename__ = 'bans'

    game_id = Column(String, ForeignKey('matches.game_id', ondelete='CASCADE'), primary_key=True)
    team_id = Column(String, primary_key=True)
    pick_turn = Column(Integer, primary_key=True)
    champion_id = Column(Integer, primary_key=True)

class Participant(Base):
    __tablename__ = 'participants'

    game_id = Column(String, ForeignKey('matches.game_id', ondelete='CASCADE'), primary_key=True)
    participant_id = Column(String, primary_key=True)

    platform_id = Column(String)
    account_id = Column(String)
    summoner_name = Column(String)
    summoner_id = Column(String)
    current_platform_id = Column(String)
    current_account_id = Column(String)
    match_history_uri = Column(String)
    profile_icon = Column(Integer)

class Stats(Base):
    __tablename__ = 'stats'

    game_id = Column(String, ForeignKey('matches.game_id', ondelete='CASCADE'), primary_key=True)
    team_id = Column(String, primary_key=True)
    participant_id = Column(String, primary_key=True)
    
    champion_id = Column(Integer)
    spell1_id = Column(Integer)
    spell2_id = Column(Integer)
    win = Column(Boolean)
    item0 = Column(Integer)
    item1 = Column(Integer)
    item2 = Column(Integer)
    item3 = Column(Integer)
    item4 = Column(Integer)
    item5 = Column(Integer)
    item6 = Column(Integer)
    kills = Column(Integer)
    deaths = Column(Integer)
    assists = Column(Integer)
    largest_killing_spree = Column(Integer)
    largest_multi_kill = Column(Integer)
    killing_sprees = Column(Integer)
    longest_time_spent_living = Column(Integer)
    double_kills = Column(Integer)
    triple_kills = Column(Integer)
    quadra_kills = Column(Integer)
    penta_kills = Column(Integer)
    unreal_kills = Column(Integer)
    total_damage_dealt = Column(Integer)
    magic_damage_dealt = Column(Integer)
    physical_damage_dealt = Column(Integer)
    true_damage_dealt = Column(Integer)
    largest_critical_strike = Column(Integer)
    total_damage_dealt_to_champions = Column(Integer)
    magic_damage_dealt_to_champions = Column(Integer)
    physical_damage_dealt_to_champions = Column(Integer)
    true_damage_dealt_to_champions = Column(Integer)
    total_heal = Column(Integer)
    total_units_healed = Column(Integer)
    damage_self_mitigated = Column(Integer)
    damage_dealt_to_objectives = Column(Integer)
    damage_dealt_to_turrets = Column(Integer)
    vision_score = Column(Integer)
    time_c_cing_others = Column(Integer)
    total_damage_taken = Column(Integer)
    magical_damage_taken = Column(Integer)
    physical_damage_taken = Column(Integer)
    true_damage_taken = Column(Integer)
    gold_earned = Column(Integer)
    gold_spent = Column(Integer)
    turret_kills = Column(Integer)
    inhibitor_kills = Column(Integer)
    total_minions_killed = Column(Integer)
    neutral_minions_killed = Column(Integer)
    neutral_minions_killed_team_jungle = Column(Integer)
    neutral_minions_killed_enemy_jungle = Column(Integer)
    total_time_crowd_control_dealt = Column(Integer)
    champ_level = Column(Integer)
    vision_wards_bought_in_game = Column(Integer)
    sight_wards_bought_in_game = Column(Integer)
    wards_placed = Column(Integer)
    wards_killed = Column(Integer)
    first_blood_kill = Column(Boolean)
    first_blood_assist = Column(Boolean)
    first_tower_kill = Column(Boolean)
    first_tower_assist = Column(Boolean)
    first_inhibitor_kill = Column(Boolean)
    first_inhibitor_assist = Column(Boolean)
    combat_player_score = Column(Integer)
    objective_player_score = Column(Integer)
    total_player_score = Column(Integer)
    total_score_rank = Column(Integer)
    player_score0 = Column(Integer)
    player_score1 = Column(Integer)
    player_score2 = Column(Integer)
    player_score3 = Column(Integer)
    player_score4 = Column(Integer)
    player_score5 = Column(Integer)
    player_score6 = Column(Integer)
    player_score7 = Column(Integer)
    player_score8 = Column(Integer)
    player_score9 = Column(Integer)
    perk0 = Column(Integer)
    perk0_var1 = Column(Integer)
    perk0_var2 = Column(Integer)
    perk0_var3 = Column(Integer)
    perk1 = Column(Integer)
    perk1_var1 = Column(Integer)
    perk1_var2 = Column(Integer)
    perk1_var3 = Column(Integer)
    perk2 = Column(Integer)
    perk2_var1 = Column(Integer)
    perk2_var2 = Column(Integer)
    perk2_var3 = Column(Integer)
    perk3 = Column(Integer)
    perk3_var1 = Column(Integer)
    perk3_var2 = Column(Integer)
    perk3_var3 = Column(Integer)
    perk4 = Column(Integer)
    perk4_var1 = Column(Integer)
    perk4_var2 = Column(Integer)
    perk4_var3 = Column(Integer)
    perk5 = Column(Integer)
    perk5_var1 = Column(Integer)
    perk5_var2 = Column(Integer)
    perk5_var3 = Column(Integer)
    perk_primary_style = Column(Integer)
    perk_sub_style = Column(Integer)
    stat_perk0 = Column(Integer)
    stat_perk1 = Column(Integer)
    stat_perk2 = Column(Integer)
    role = Column(String)
    lane = Column(String)

class TimelineParticipant(Base):
    __tablename__ = 'timeline_participants'

    game_id = Column(String, ForeignKey('matches.game_id', ondelete='CASCADE'), primary_key=True)
    timestamp = Column(Integer, primary_key=True)
    participant_id = Column(String, primary_key=True)

    current_gold = Column(Integer)
    total_gold = Column(Integer)
    level = Column(Integer)
    xp = Column(Integer)
    minions_killed = Column(Integer)
    jungle_minions_killed = Column(Integer)
    dominion_score = Column(Integer)
    team_score = Column(Integer)
    position_x = Column(Integer)
    position_y = Column(Integer)

class TimelineEvents(Base):
    __tablename__ = 'timeline_events'

    game_id = Column(String, ForeignKey('matches.game_id', ondelete='CASCADE'), primary_key=True)
    timestamp = Column(Integer, primary_key=True)
    participant_id = Column(String, primary_key=True)
    type = Column(String, primary_key=True)
    sequence = Column(Integer, primary_key=True)

    skill_slot = Column(String)
    level_up_type = Column(String)
    item_id = Column(String)
    ward_type = Column(String)
    creator_id = Column(String)
    killer_id = Column(String)
    victim_id = Column(String)
    assisting_participant_ids = Column(String)
    position_x = Column(String)
    position_y = Column(String)
    monster_type = Column(String)
    after_id = Column(String)
    before_id = Column(String)
    team_id = Column(String)
    building_type = Column(String)
    lane_type = Column(String)
    tower_type = Column(String)
    monster_sub_type = Column(String)

class LeagueDB:
    def __init__(self, con: str, api_key: str):
        self.engine = create_engine(con)
        self.Session = sessionmaker(bind=self.engine)
        self.api = api.RiotApi(api_key)

    def create_db_layout(self) -> None:
        Base.metadata.create_all(self.engine)

    def update_summoner(self, summoner_name: str, number_of_games: int=100, champion_id: int=-1, season_id: str=-1, patch: str=-1, begin_time: datetime=None, queue_id: int=-1) -> None:
        api.logging.info('update summoner: {0}'.format(summoner_name))
        session = self.Session()
        try:
            df_summoner = self.api.get_summoner_by_name(summoner_name)
            if df_summoner.empty:
                api.logging.info('summoner with name {0} not found'.format(summoner_name))
                return
            
            summoner = Summoner(**df_summoner.reset_index().iloc[0])
            session.merge(summoner)
            session.commit()

            try:
                matches = self.api.get_match_list(summoner.account_id, champion_id=champion_id, end_index=number_of_games, begin_time=begin_time, queue_id=queue_id)
                if matches.empty:
                    api.logging.info('no new matches for summoner {0}'.format(summoner_name))
                    return

                query = session.query(Match).filter(Match.game_id.in_([str(n) for n in matches.game_id.values])) 
                matches_already_loaded = pd.read_sql(sql=query.statement, con=session.bind)
                if matches_already_loaded.empty:
                    new_matches = matches.game_id.values
                else:
                    new_matches = matches[~matches.game_id.isin(matches_already_loaded.game_id)].game_id.values

                api.logging.info('{0} out of {1} are new matches'.format(len(new_matches), len(matches)))
                for match in new_matches:
                    try:
                        details = self.api.get_match_details(match)
                        for name, table in details.items():
                            try:
                                table.to_sql(name=name, con=session.bind, if_exists='append')
                            except IntegrityError:
                                pass
                        
                        timeline = self.api.get_timeline(match)
                        for name, table in timeline.items():
                            table.to_sql(name=name, con=session.bind, if_exists='append')
                        
                        api.logging.info('Merged {0} successfully'.format(match))
                    except Exception as e:
                        api.logging.error('error while gathering match details for game_id {0}'.format(match))
                        api.logging.error(str(e))
                        pass
            except NoResultFound as e:
                api.logging.info('no new matches for summoner {0}'.format(summoner_name))
                pass
            except Exception as e:
                api.logging.error('error while gathering game_id data for summoner {0}'.format(summoner_name))
                api.logging.error(str(e))


        except Exception as e:
            api.logging.error('error while gathering summoner data for summoner {0}'.format(summoner_name))
            api.logging.error(str(e))
            pass

    def update_static_data(self) -> None:
        self._update_challenger_leaderboard()
        print('leaderboards have been created')
        self._update_champions()
        print('champions have been created')
        self._update_queue_types()
        print('queues have been updated')

    def _update_challenger_leaderboard(self) -> None:
        solo = self.api.get_leaderboard(api.QueueType.RANKED_SOLO)
        solo.to_sql(name='leaderboard_solo', con=self.engine, if_exists='replace')

        flex = self.api.get_leaderboard(api.QueueType.RANKED_FLEX)
        flex.to_sql(name='leaderboard_flex', con=self.engine, if_exists='replace')

    def _update_champions(self) -> None:
        champions = self.api.get_champion_json()
        champions.to_sql(name='champions', con=self.engine, if_exists='replace')
    
    def _update_queue_types(self) -> None:
        queues = self.api.get_queue_types()
        queues.to_sql(name='queues', con=self.engine, if_exists='replace')