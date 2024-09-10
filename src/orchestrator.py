from dao import JobRunsDAO,DatasetDAO,DataFlowsDAO,JobDAO
from time import sleep
from datetime import datetime,timedelta
import threading
import pytz
import logging.config
from utils import get_config_file_path
from simulator import SqsQueue
from parameters import Parameters
# Load the logging configuration

logging.config.fileConfig(get_config_file_path('logging.conf'))

# Create a logger
logger = logging.getLogger()



class Consumer:
    def __init__(self) -> None:
        self.jobDAO = JobDAO()
        self.jobRunsDAO = JobRunsDAO()
        self.datasetDAO = DatasetDAO()
        self.dataFlowsDAO = DataFlowsDAO()
    """
    recibe un mensaje, que contiene la información de un proceso, 
    lo parsea y lo registra en el histórico de ejecuciones de procesos
    para los casos en los que se incluya el campo dataset, se enviará también como resultado
    """
    def register_job(self,event):
        logging.info(event)
        job=self.jobDAO.get_by_name(event['job_name'])
        jobruns={**event,'created_at':event['timestamp'],'id':job['id']}
        jobruns.pop('timestamp')
        jobruns.pop('job_name')
        dataset = jobruns.get('dataset')
        if dataset is not None:
            jobruns.pop('dataset')
        self.jobRunsDAO.register(**jobruns)
        return (jobruns,dataset)
    
    """
    actualiza el timestamp de un dataset específico
    """
    def update_dataset(self, dataset_id,timestamp):
        self.datasetDAO.update(id=dataset_id,updated_at=timestamp)

    """
    actualiza el timestamp de los datasets que han sido modificados por el proceso
    devuelve el conjunto de datasets actualizados
    """
    def update_datasets(self, job_id, timestamp):
        res = []
        output_datasets = self.dataFlowsDAO.get_output_datasets_by_job_id(job_id)
        for dataset in output_datasets:
            #DatasetDAO.update(id=dataset['id'],updated_at=timestamp)
            self.update_dataset(dataset['id'],timestamp)
            data={**dataset, 'updated_at':timestamp}
            res.append(data)
        return res

class Trigger:

    def __init__(self):
        self.params= Parameters()
        self.id_days=self.params.get_parameter_id('days')
        self.id_hours=self.params.get_parameter_id('hours')
        self.timezone = pytz.timezone('Europe/Madrid')

    def is_dataset_ready(self, dataset):
        updated_at=dataset['updated_at']
        if (dataset['freshness_unit'] == self.id_days):
            time_diff = datetime.now(self.timezone) - timedelta(days=dataset['freshness_period'])
        elif (dataset['freshness_unit'] == self.id_hours):   
            time_diff = datetime.now(self.timezone) - timedelta(hours=dataset['freshness_period'])
        else:
            logging.error("error")      
            time_diff =None  

        return updated_at > time_diff

    def trigger_events(self, datasets):
        jobs=DataFlowsDAO().get_output_jobs_by_dataset_id([dataset['id'] for dataset in datasets])
        for job in jobs:
            if job['is_ready']:
                logging.info('execute job: {}'.format(job['name']))
                SqsQueue().write_events_to_sqs(job['name'],True)
            elif bool(job['cron']):
                logging.info('cron job no se ejecuta por eventos')
            else:
                input_datasets= DataFlowsDAO.get_input_datasets_by_job_id(job['id'])
                ready=all(self.is_dataset_ready(dataset) for dataset in input_datasets)
                if ready:
                    logging.info('execute job: {}'.format(job['name']))
                    SqsQueue().write_events_to_sqs(job['name'],True)
                    

if __name__=="__main__":

    def reader():
        while True:
            try:
                #recibe mensajes con el resumen de la ejecución de un proceso desde una cola
                messages=SqsQueue().read_events_from_sqs()
                for msg in messages:
                    #parsea y registra el proceso
                    (job,dataset) = Consumer().register_job(msg)
                    """
                    si el proceso finalizó correctamente:
                        se devuelve la lista de datasets actualizados y a partir de ellos se identifican los siguientes
                        procesos que serán desencadenados
                    si el proceso tuvo algún tipo de error:
                        se registra el error para activar las notificaciones oportunas para solucionar el problema
                    """
                    if job['is_ok']:
                        if len(dataset)>0:
                            dataset=DatasetDAO.get_by_name(dataset)
                            Consumer().update_dataset(dataset['id'], job['created_at'])
                            Trigger().trigger_events([dataset])
                        else:
                            datasets = Consumer().update_datasets(job['id'], job['created_at'])
                            Trigger().trigger_events(datasets)
                    else:
                        logging.error(job['error_code'], job['error_description'])
            except Exception as e:
                logging.error(f"Exception: {e}") 
            sleep(1)

    

    events = threading.Thread(target=reader)
    events.start()

    while True:
        """
        se simula la ejecución de procesos, utilizando el framework frink para actualizar tablas de staging 
        que desencadene el resto de procesos
        """
        SqsQueue().write_events_to_sqs('frink',True)
        sleep(10)

