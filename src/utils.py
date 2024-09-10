from pathlib import Path
from configparser import ConfigParser
from csv import DictReader
from yaml import safe_load
import json

def get_file_path(folder,filename):
    base_path = Path(__file__).parent
    return (base_path / "../{}/{}".format(folder,filename)).resolve()

def get_config_file_path(filename):
    return get_file_path('config',filename)

def get_data_file_path(filename):
    return get_file_path('data',filename)

def read_db_config(section='postgresql'):
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

def loadFromCSV(filename):
    with open(get_data_file_path("{}.csv".format(filename)), 'r') as file:
        reader = DictReader(file)
        return list(reader)

# Function to list all databases
def list_databases(glue):
    response = glue.get_databases()
    databases = response['DatabaseList']
    return [db for db in databases]

# Function to list all tables in a specific database
def list_tables(glue,data_source):
    response = glue.get_tables(DatabaseName=data_source.name)
    tables = response['TableList']
    return [{**table,'datasource':data_source.id }for table in tables]

def parse_glue_datasource(_dict, layers):
    res={}
    res['name']=_dict['Name']
    res['catalog_id']=_dict['CatalogId']
    if '_ld' in _dict['Name']:
        res['layer'] = layers['landing']
    elif '_st' in _dict['Name']:
        res['layer'] = layers['staging']
    elif '_bl' in _dict['Name']:
        res['layer'] = layers['business']
    return res

def parse_glue_dataset(_dict):
    res={}
    res['name']=_dict['Name'][:50]
    res['datasource']=_dict['datasource']
    return res

def parse_glue_dataset_stats(_dict):
    res={}
    res['name']=_dict['Name'][:50]
    _dict['CreateTime']=_dict['CreateTime'].strftime("%Y-%m-%d %H:%M:%S")
    _dict['UpdateTime']=_dict['UpdateTime'].strftime("%Y-%m-%d %H:%M:%S")
    if 'LastAccessTime' in _dict:
        _dict['LastAccessTime']=_dict['LastAccessTime'].strftime("%Y-%m-%d %H:%M:%S")
    res['stats'] = json.dumps(_dict)
    return res

def from_yml_config():
    with open(get_config_file_path('parameters.yml'), 'r') as file:
        return safe_load(file)

