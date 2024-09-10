from model import init,Dataset,Job,DataFlows,DataSource,DatasetStats
from connection import DatabaseConnection
import utils 
import boto3
from parameters import Parameters
class Loader:

    def __init__(self):
        self.params = Parameters()

    def initModel(self):
        self.session=DatabaseConnection().get_session()
        self.datasets=[]
        self.dataset_stats=[]
        init()

    def load_from_glue_datacatalog(self):
        glue = boto3.client('glue')
        databases = [db for db in utils.list_databases(glue) if 'db_cep_' in db['Name']]
        self.datasources=[DataSource.from_dict(utils.parse_glue_datasource(db,self.params.get_parameters_by_category('layer'))) for db in databases]
        self.session.add_all(self.datasources)
        self.session.commit()

        # Get all tables for each database
        for ds in self.datasources:
            tables = utils.list_tables(glue,ds)
            self.datasets = self.datasets + [Dataset.from_dict(utils.parse_glue_dataset(table)) for table in tables]
            self.dataset_stats = self.dataset_stats + [DatasetStats.from_dict(utils.parse_glue_dataset_stats(table)) for table in tables]
        self.session.add_all(self.datasets)
        self.session.add_all(self.dataset_stats)
        self.session.commit()

    def loadJobsFromCSV(self):
        self.jobs=[Job().from_dict(d) for d in [{**f, 'is_ready': bool(f['is_ready']), 'framework':self.params.get_parameter_id(f['framework'])} for f in utils.loadFromCSV('jobs')]]
        self.session.add_all(self.jobs)
        self.session.commit()


    def loadDataFlowsSamplefromDB(self):
        datasource_st=[ds for ds in self.datasources if ds.layer==self.params.get_parameter_id('staging')][0]
        datasource_bl=[ds for ds in self.datasources if ds.layer==self.params.get_parameter_id('business')][0]

        datasets_st=[ds for ds in self.datasets if ds.datasource==datasource_st.id]
        datasets_bl=[ds for ds in self.datasets if ds.datasource==datasource_bl.id]
        
        frink_job=[job for job in self.jobs if job.framework==self.params.get_parameter_id('frink')][0]
        dbt_jobs=[job for job in self.jobs if job.framework==self.params.get_parameter_id('dbt')][0:3]
        dataflows=[DataFlows().from_dict({'job_id':frink_job.id, 'dataset_id':d.id, 'operation':self.params.get_parameter_id('write')}) for d in datasets_st ]
        dataflows= dataflows + [DataFlows().from_dict({'job_id':dbt_jobs[0].id, 'dataset_id':datasets_st[0].id, 'operation':self.params.get_parameter_id('read')}),
                                DataFlows().from_dict({'job_id':dbt_jobs[0].id, 'dataset_id':datasets_st[1].id, 'operation':self.params.get_parameter_id('read')}),
                                DataFlows().from_dict({'job_id':dbt_jobs[0].id, 'dataset_id':datasets_bl[0].id, 'operation':self.params.get_parameter_id('write')})]
        dataflows= dataflows + [DataFlows().from_dict({'job_id':dbt_jobs[1].id, 'dataset_id':datasets_st[1].id, 'operation':self.params.get_parameter_id('read')}),
                                DataFlows().from_dict({'job_id':dbt_jobs[1].id, 'dataset_id':datasets_st[2].id, 'operation':self.params.get_parameter_id('read')}),
                                DataFlows().from_dict({'job_id':dbt_jobs[1].id, 'dataset_id':datasets_bl[1].id, 'operation':self.params.get_parameter_id('write')})]
        dataflows= dataflows + [DataFlows().from_dict({'job_id':dbt_jobs[2].id, 'dataset_id':datasets_bl[0].id, 'operation':self.params.get_parameter_id('read')}),
                                DataFlows().from_dict({'job_id':dbt_jobs[2].id, 'dataset_id':datasets_bl[1].id, 'operation':self.params.get_parameter_id('read')}),
                                DataFlows().from_dict({'job_id':dbt_jobs[2].id, 'dataset_id':datasets_bl[2].id, 'operation':self.params.get_parameter_id('write')})]

        self.session.add_all(dataflows)
        self.session.commit()



if __name__ == "__main__":
    loader = Loader()
    loader.initModel()
    loader.load_from_glue_datacatalog()
    loader.loadJobsFromCSV()
    loader.loadDataFlowsSamplefromDB()