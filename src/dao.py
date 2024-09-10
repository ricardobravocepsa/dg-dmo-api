from sqlalchemy import and_, desc
from typing import TypeVar, Generic, Type
from model import Job, Dataset, JobRuns, DataFlows,DataSource
from connection_pool import DatabaseConnection
from parameters import Parameters

T = TypeVar('T')

params = Parameters()

class BaseDAO(Generic[T]):
    model: Type[T]
    db = DatabaseConnection()

    @classmethod
    def create(cls, **kwargs) -> T:
        with cls.db.transaction() as session:
            obj = cls.model(**kwargs)
            session.add(obj)
            session.flush()
            return obj

    @classmethod
    def get_by_id(cls, id: int) -> dict:
        with cls.db.get_session() as session:
            job= session.query(cls.model).filter(cls.model.id == id).first()
            if job:        
                res = job.to_dict()
            else:
                res={}
            return res 
    @classmethod
    def get_by_name(cls, name: str) -> dict:
        with cls.db.get_session() as session:
            job= session.query(cls.model).filter(cls.model.name == name).first()
            if job:        
                res = job.to_dict()
            else:
                res={}
            return res 

    @classmethod
    def get_all(cls) -> list[dict]:
        with cls.db.get_session() as session:
            return [item.to_dict() for item in session.query(cls.model).all()]

    @classmethod
    def update(cls, id: int, **kwargs) -> T:
        with cls.db.transaction() as session:
            obj = session.query(cls.model).filter(cls.model.id == id).first()
            if obj:
                for key, value in kwargs.items():
                    setattr(obj, key, value)
            return obj

    @classmethod
    def delete(cls, id: int) -> bool:
        with cls.db.transaction() as session:
            obj = session.query(cls.model).filter(cls.model.id == id).first()
            if obj:
                session.delete(obj)
                return True
            return False

        
class DataSourceDAO(BaseDAO[DataSource]):
    model = DataSource  
    def filter_by_layer(cls, layer: int) -> list:
        with cls.db.get_session() as session:
            data= session.query(cls.model).filter(cls.model.layer == layer).all()
            if data:        
                res = [d.to_dict() for d in data]
            else:
                res=[]
            return res     

class DatasetDAO (BaseDAO[Dataset]):
    model = Dataset
    def get_by_layer(cls, layer: int) -> dict:
        with cls.db.get_session() as session:
            datasources = [ds['id'] for ds in DataSourceDAO().filter_by_layer(layer)]
            return [d.to_dict() for d in session.query(Dataset).filter(Dataset.datasource.in_(datasources)).all()]
        
class JobDAO (BaseDAO[Job]):
    model = Job        

class JobRunsDAO (BaseDAO[JobRuns]):
    model = JobRuns

    @classmethod
    def update_last(cls, id: int, **kwargs) -> T:
        with cls.db.transaction() as session:
            obj = session.query(cls.model).filter(cls.model.id == id).order_by(desc(JobRuns.created_at)).first()
            if obj:
                for key, value in kwargs.items():
                    setattr(obj, key, value)
            return obj
        
    @classmethod
    def register(cls, **kwargs) -> None:
        with cls.db.transaction() as session:
            cls.update_last(id=kwargs['id'],is_current=False)
            cls.create(**kwargs)
    @classmethod
    def get_current_job_runs(cls) -> dict:
        with cls.db.get_session() as session:            
            res=session.query(JobRuns).filter(JobRuns.is_current is True).all()
            return [row.to_dict() for row in res]     

class DataFlowsDAO (BaseDAO[DataFlows]):

    operation_write_id= params.get_parameter_id('write')
    operation_read_id= params.get_parameter_id('read')
    model = DataFlows

    def get_datasets_by_job_id(cls, job_id: int, operation: int) -> dict:
         with cls.db.get_session() as session:            
            res=session.query(DataFlows).filter(and_(DataFlows.job_id == job_id,DataFlows.operation == operation)).join(Dataset).all()
            return [row.datasets.to_dict() for row in res]

    @classmethod
    def get_input_datasets_by_job_id(cls, job_id: int) -> dict:
        return cls.get_datasets_by_job_id(cls, job_id, cls.operation_read_id)
        
    @classmethod
    def get_output_datasets_by_job_id(cls, job_id: int) -> dict:
        return cls.get_datasets_by_job_id(cls, job_id, cls.operation_write_id)
        
    @classmethod
    def get_output_jobs_by_dataset_id(cls, dataset_ids) -> dict:
        with cls.db.get_session() as session:            
            res=session.query(DataFlows).filter(and_(DataFlows.dataset_id.in_(dataset_ids),DataFlows.operation == cls.operation_read_id)).join(Job).distinct(Job.id).all()
            return [row.jobs.to_dict() for row in res]        