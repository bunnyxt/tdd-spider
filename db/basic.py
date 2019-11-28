from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from conf import get_db_args

__all__ = ['engine', 'Base', 'Session', 'create_all', 'drop_all', 'update_engine']


def get_engine():
    db_args = get_db_args()
    conn_str = "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8".format(
        db_args['user'], db_args['password'], db_args['host'], db_args['port'], db_args['dbname'])  # mysql
    eng = create_engine(conn_str, encoding='utf-8', pool_recycle=7200)
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
