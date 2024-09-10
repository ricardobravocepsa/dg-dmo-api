import logging.config
import boto3
from utils import get_config_file_path
from datetime import datetime
from random import randint,choice
import json
from dao import DatasetDAO
from parameters import Parameters

logging.config.fileConfig(get_config_file_path('logging.conf'))

# Create a logger
logger = logging.getLogger()

class SqsQueue:
    def __init__(self) -> None:
        self.queue_url='https://sqs.eu-west-1.amazonaws.com/071350574062/digital-datalake-sqs-job-events'
        self.sqs_client = boto3.client('sqs')
        self.parameters= Parameters()
        self.datasets=[dataset['name'] for dataset in DatasetDAO().get_by_layer(self.parameters.get_parameter_id('staging'))]

    def write_events_to_sqs(self,name,is_ok,error_code=None,error_description=None):

        dataset = ''
        if name == 'frink':
            dataset=choice(self.datasets)
        message_body = {'job_name': name, 'is_ok':is_ok, 
                        'timestamp':datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],
                        'duration':randint(10,10000),
                        'dataset':dataset}
        if error_code and error_description:
            message_body['error_code']=error_code
            message_body['error_description']=error_description

        response = self.sqs_client.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(message_body))
        logging.info(f"Message sent with MessageId: {response['MessageId']}")

    def read_events_from_sqs(self):
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,  # Maximum number of messages to retrieve
            WaitTimeSeconds=20  # Wait time for long polling (optional)
        )
        events=[]
        for message in response.get('Messages', []):
            message_body = message['Body']
            receipt_handle = message['ReceiptHandle']

            # Process the message body (e.g., parse JSON, extract relevant data)
            logging.info(f"Received message: {message_body}")
            events.append(json.loads(message_body))
            # Delete the message from the queue
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )

        logging.info("All messages processed and deleted.")

        return events
