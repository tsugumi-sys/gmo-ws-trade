import sqlalchemy
from sqlalchemy.orm import sessionmaker

# Database item
test_engine = sqlalchemy.create_engine("sqlite:///:memory:")

SessionLocal = sessionmaker(bind=test_engine, autocommit=False, autoflush=False)
