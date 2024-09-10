import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
import threading
import time
from utils import read_db_config

class DatabaseConnection:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabaseConnection, cls).__new__(cls)
                cls._instance._initialize()
            return cls._instance

    def _initialize(self):
        self._create_engine()
        self._setup_session_factory()



    def _create_engine(self):
        config = read_db_config()
        db_url = os.getenv('DATABASE_URL', f"postgresql://{config['user']}:{config['password']}@{config['host']}/{config['database']}")
        self.engine = create_engine(
            db_url,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,  # Recycle connections after 30 minutes
            pool_pre_ping=True,  # Enable connection health checks
            connect_args={"options": f"-csearch_path={config['schema']}"}
        )

        # Set idle_in_transaction_session_timeout for each connection
        @event.listens_for(self.engine, "connect")
        def set_session_timeout(dbapi_connection, connection_record):
            with dbapi_connection.cursor() as cursor:
                cursor.execute(f"SET idle_in_transaction_session_timeout = '{config['idle_in_transaction_session_timeout']}';")  # 5 minutes

    def _setup_session_factory(self):
        self.SessionFactory = scoped_session(sessionmaker(bind=self.engine))

    def getSessionFactory(self):
        return self.SessionFactory()

    @contextmanager
    def get_session(self):
        session = self.getSessionFactory()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_engine(self):
        return self.engine

    def check_connection_health(self):
        try:
            with self.get_session() as session:
                session.execute("SELECT 1")
            return True
        except SQLAlchemyError:
            return False

    def close_connections(self):
        self.SessionFactory.remove()
        self.engine.dispose()

    def recreate_pool(self):
        self.close_connections()
        self._create_engine()
        self._setup_session_factory()

    @contextmanager
    def transaction(self):
        with self.get_session() as session:
            try:
                yield session
                session.commit()
            except:
                session.rollback()
                raise

    def execute_with_retry(self, operation, max_retries=3, retry_delay=1):
        retries = 0
        while retries < max_retries:
            try:
                return operation()
            except SQLAlchemyError as e:
                retries += 1
                if retries == max_retries:
                    raise e
                time.sleep(retry_delay)
                self.recreate_pool()  # Recreate the pool before retrying
