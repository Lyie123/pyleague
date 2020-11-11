import pandas as pd
import league_api as api
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
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