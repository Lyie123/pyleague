import pandas as pd
import league_api as api
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship

Base = declarative_base()

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

    game_id = Column(String, ForeignKey('matches.game_id'), primary_key=True)
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

    game_id = Column(String, ForeignKey('matches.game_id'), primary_key=True)
    team_id = Column(String, primary_key=True)
    pick_turn = Column(Integer, primary_key=True)
    champion_id = Column(Integer, primary_key=True)

class Participant:
    __tablename__ = 'participants'

    game_id = Column(String, ForeignKey('matches.game_id'), primary_key=True)
    participant_id = Column(String, primary_key=True)

    platform_id = Column(String)
    account_id = Column(String)
    summoner_name = Column(String)
    summoner_id = Column(String)
    current_platform_id = Column(String)
    current_accout_id = Column(String)
    match_history_uri = Column(String)
    profile_icon = Column(Integer)

class Stats(Base):
    __tablename__ = 'stats'

    game_id = Column(String, ForeignKey('matches.game_id'), primary_key=True)
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



def create_db_layout(connection_string: str):
    engine = create_engine(connection_string, echo=True)
    Base.metadata.create_all(engine)