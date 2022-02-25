from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base

# "postgresql+psycopg2://repl:S3cretPassw0rd@localhost:5432/campdata_prod"
engine = create_engine(    
  "postgresql+psycopg2://anjali:{password}@postgresql-dataanalytics.postgres.database.azure.com/postgres?sslmode=require"    
)
session = Session(engine)
Base = declarative_base()

def create_all():
  Base.metadata.create_all(engine, checkfirst = True)

