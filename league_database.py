import pandas as pd
import league_api as api
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Match(Base):
    __tablename__ = 'matches'

    platform_id = Column(String, primary_key=True)
    game_id = Column(String, primary_key=True)

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

    game_id = Column(String, primary_key=True)
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





def create_db_layout(connection_string: str):
    engine = create_engine(connection_string, echo=True)
    Base.metadata.create_all(engine)