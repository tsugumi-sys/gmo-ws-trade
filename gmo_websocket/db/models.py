from sqlalchemy import Column, Integer, Float, String

from db.database import Base


class Board(Base):
    __tablename__ = "board"

    # Generate id using uuid
    id = Column(String(128), primary_key=True)
    # Unix timestamp (ms) should be int converted from isoformat string
    timestamp = Column(Integer, index=True)
    price = Column(Float, index=True)
    size = Column(Float)
    side = Column(String(10), index=True)
    symbol = Column(String(10), index=True)


class Tick(Base):
    __tablename__ = "tick"

    # generate id using uuid
    id = Column(String(128), primary_key=True)
    # Unix timestamp (ms) should be int converted from isoformat string
    timestamp = Column(Integer, index=True)
    price = Column(Float, index=True)
    size = Column(Float)
    symbol = Column(String(10), index=True)


class OHLCV(Base):
    __tablename__ = "ohlcv"

    timestamp = Column(Integer, primary_key=True, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    symbol = Column(String(10), index=True)
