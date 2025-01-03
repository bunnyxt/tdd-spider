from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from conf import get_db_args

__all__ = ['engine', 'Base', 'Session',
           'create_all', 'drop_all', 'update_engine']


def get_engine():
    db_args = get_db_args()
    conn_str = "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4&connect_timeout=10&read_timeout=30&write_timeout=60".format(
        db_args['user'], db_args['password'], db_args['host'], db_args['port'], db_args['dbname'])  # mysql
    eng = create_engine(conn_str, pool_recycle=3600, pool_size=50,
                        max_overflow=150, pool_timeout=60, pool_pre_ping=True)
    return eng


def update_engine():
    global engine, Base, Session
    engine = get_engine()
    Base = declarative_base()
    Session = sessionmaker(bind=engine)


def create_all():
    # create table in db
    # do not need to use it if db already created these tables
    Base.metadata.create_all(engine)


def drop_all():
    # drop table in db
    # WARNING do not use it unless you are sure to drop all table
    Base.metadata.drop_all(engine)


engine = get_engine()
Base = declarative_base()
Session = sessionmaker(bind=engine)
# db_session = Session()
