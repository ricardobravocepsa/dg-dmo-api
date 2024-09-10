from sqlalchemy.orm import declarative_base,relationship,mapped_column
from sqlalchemy import Column, Integer, String, SmallInteger, VARCHAR, Boolean,DateTime, MetaData, ForeignKey, PrimaryKeyConstraint, Index, inspect, event,func
from sqlalchemy.dialects.postgresql import JSONB
from connection_pool import DatabaseConnection
from sqlalchemy.schema import CreateSchema,DropSchema
import utils
import yaml
from parameters import Parameters

schema_name = utils.read_db_config()['schema']

metadata = MetaData(schema=schema_name)

Base = declarative_base()

params = Parameters()

class BaseModel(Base):
    __abstract__ = True  # This ensures no table is created for this class
    __table_args__ = {'schema': schema_name}
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)
    
    def to_dict(self):
        return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}
    
    def from_yml_config(self,table):
        data={}
        with open(utils.get_config_file_path('parameters.yml'), 'r') as file:
            data = yaml.safe_load(file)
            values = data.get(table,[])
            return values
        
class Job(BaseModel):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column (String, nullable=False)
    framework = Column(SmallInteger)
    arn_step_function = Column(String)
    iniciativa = Column(VARCHAR(20))
    is_ready = Column(Boolean)
    params = Column(JSONB)
    cron = Column(VARCHAR(10))

    dataflows = relationship ("DataFlows", back_populates="jobs")

class DataSource(BaseModel):
    __tablename__ = "datasources"
    id = Column(SmallInteger, primary_key=True, autoincrement=True)
    name = Column (VARCHAR(50))
    layer = Column (SmallInteger)
    catalog_id =  Column (VARCHAR(12))


class Dataset(BaseModel):
    __tablename__="datasets"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(50), nullable=False, unique=True)
    updated_at = mapped_column(DateTime(timezone=True),server_default=func.now(),onupdate=func.now())
    datasource = Column (SmallInteger)
    truedat_id = Column(Integer)
    freshness_unit = Column(SmallInteger, default=params.get_parameter_id('days'))
    freshness_period = Column (SmallInteger, default=1)
    freshness_threshold = Column (SmallInteger, default=1)
    snapshots = Column (SmallInteger, default=0)

    dataflows = relationship ("DataFlows", back_populates="datasets")

class DatasetStats(BaseModel):
    __tablename__="dataset_stats"
    name = Column(VARCHAR(50), ForeignKey(Dataset.name), primary_key=True)
    stats = Column (JSONB)

class DataFlows(BaseModel):
    __tablename__="data_flows"
    job_id = Column (Integer, ForeignKey (Job.id), nullable=False)
    dataset_id = Column (Integer, ForeignKey (Dataset.id), nullable=False)
    operation = Column (SmallInteger, nullable=False)

    jobs = relationship("Job", back_populates="dataflows")
    datasets = relationship("Dataset", back_populates="dataflows")

    __table_args__ = (
        PrimaryKeyConstraint('job_id', 'dataset_id'),
        Index ('ix_operation', 'operation'),
        Index ('ix_job_id', 'job_id'),
        Index ('ix_dataset_id', 'dataset_id'),
        Index ('ix_dataset_id_job_id', 'dataset_id', 'job_id'),
    	Index ('ix_dataset_id_operation', 'job_id', 'operation'),
        Index ('ix_job_id_operation', 'dataset_id', 'operation')
    )


class JobRuns(BaseModel):
    __tablename__="job_runs"
    id = Column(Integer, ForeignKey (Job.id) )
    error_code = Column (SmallInteger, default=None)
    error_description = Column (String, default=None)
    is_ok = Column(Boolean)
    duration = Column (Integer)
    is_current = Column (Boolean, default=True)
    created_at = Column(DateTime)

    __table_args__ = (
        PrimaryKeyConstraint('id', 'created_at'),
        Index ('ix_current', 'is_current')
    )


def init():
    engine = DatabaseConnection().get_engine()
    
    inspector = inspect(engine)
    if schema_name in inspector.get_schema_names():
        with engine.connect() as connection:
            connection.execute(DropSchema(schema_name,cascade=True))
            connection.execute(CreateSchema(schema_name))
            connection.commit()
    else:
        with engine.connect() as connection:
            connection.execute(CreateSchema(schema_name))
            connection.commit()

    Base.metadata.create_all(engine)
    
if __name__ == "__main__":
    init()
    
    