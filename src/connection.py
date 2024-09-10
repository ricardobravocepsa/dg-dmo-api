from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from configparser import ConfigParser
from contextlib import contextmanager
from utils import get_config_file_path

class DatabaseConnection:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.engine = None
            cls._instance.SessionLocal = None
        return cls._instance
    
    def connect(self):
        if not self.engine:
            config = self._read_db_config()
            db_url = f"postgresql://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
            self.engine = create_engine(db_url, connect_args={"options": f"-csearch_path={config['schema']}"})
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def _read_db_config(self, section='postgresql'):
        filename = get_config_file_path('database.ini')
        parser = ConfigParser()
        parser.read(filename)
        db_config = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db_config[param[0]] = param[1]
        else:
            raise Exception(f'Section {section} not found in the {filename} file')
        return db_config
    
    def get_engine(self):
        if not self.engine or not self.SessionLocal:
            self.connect()
        return self.engine
    
    def get_session(self):
        if not self.engine or not self.SessionLocal:
            self.connect()
        return self.SessionLocal()
    
    def close_connection(self):
        if self.engine:
            self.engine.dispose()
        DatabaseConnection._instance = None

@contextmanager
def get_db_session():
    db = DatabaseConnection()
    session = db.get_session()
    try:
        yield session
        session.commit()
    except SQLAlchemyError:
        session.rollback()
        raise
    finally:
        session.close()

@contextmanager
def get_db_engine():
    db = DatabaseConnection()
    return db.get_engine()    