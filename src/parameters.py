import boto3
from utils import from_yml_config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
class Parameters:

    def __init__(self, table_name='dg-parameters', region_name='eu-west-1'):
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb.Table(table_name)
        self.load_from_yml_config()

    def load_from_yml_config(self):
        categories = from_yml_config()
        index=0
        for category in categories:
            for name in categories[category]:
                self.add_param(name,category,index)
                index+=1

    def add_param(self, name, category, id):
        try:
            self.table.put_item(
                Item={
                    "name": name,
                    "category": category,
                    "id": id,
                }
            )
        except ClientError as err:
            print(err)
            raise        
    
    def get_parameter(self, param_name):
        try:
            response = self.table.get_item(Key={'name': param_name})
            item = response.get('Item')
            if item:
                return item
        except ClientError as err:
            print(err)
            raise   
    def get_parameter_id(self, param_name):       
        return int(self.get_parameter(param_name)['id'])
    
    def get_parameters_by_category(self, category):
        try:
            response = self.table.scan(
                FilterExpression=Attr('category').eq(category)
            )
            items = {item['name']:int(item['id']) for item in response.get('Items', [])}
            return items
        except ClientError as err:
            print(err)
            raise   

if __name__ == "__main__":
    param=Parameters()
    param.get_parameters('operation')
    