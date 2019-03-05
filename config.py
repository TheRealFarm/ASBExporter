import logging
import yaml
import os
from logging.handlers import RotatingFileHandler

class Config(object):
    def __init__(self, client_id, client_secret, subscription_id, tenant_id, resource_group, namespace, log_level, publish_port, collectors):
        self.client_id = client_id
        self.client_secret = client_secret
        self.subscription_id = subscription_id
        self.tenant_id = tenant_id
        self.resource_group = resource_group
        self.namespace = namespace
        self.log_level = log_level
        self.publish_port = publish_port
        self.collectors = collectors

default_config = {
    'AZURE_CLIENT_ID': None,
    'AZURE_CLIENT_SECRET': None,
    'AZURE_SUBSCRIPTION_ID': None,
    'AZURE_TENANT_ID': None,
    'AZURE_RESOURCE_GROUP': None,
    'AZURE_NAMESPACE': None,
    'EXPORTER_LOG_LEVEL': 'info',
    'PUBLISH_PORT': 9145,
    'COLLECTORS': 'Namespace,Queue'
}

def init_config(script_dir, args):
    config = default_config
    config_file_path = os.path.join(script_dir, 'config.yaml')
    with open(config_file_path, 'r') as yamlfile:
        config_file = yaml.load(yamlfile)
    
    for key in config:
        if key.startswith('AZURE'):
            if key.lower() in config_file:
                config[key] = config_file[key.lower()] 
            elif os.getenv(key):
                config[key] = os.getenv(key)
            else:
                attr = key.lower()[6:]
                if hasattr(args, attr):
                    config[key] = getattr(args, attr)
                else:
                    raise RuntimeError('{0} not in config.yaml, set in environment variables or passed as an argument'.format(key))
    
    if 'log_level' in config_file:
        config['EXPORTER_LOG_LEVEL'] = config_file['log_level']
    elif os.getenv('EXPORTER_LOG_LEVEL'):
        config['EXPORTER_LOG_LEVEL'] = os.getenv('EXPORTER_LOG_LEVEL')
    elif args.log_level:
        config['EXPORTER_LOG_LEVEL'] = args.log_level

    if 'publish_port' in config_file:
        config['PUBLISH_PORT'] = config_file['publish_port']
    elif os.getenv('PUBLISH_PORT'):
        config['PUBLISH_PORT'] = os.getenv('PUBLISH_PORT')
    elif args.port:
        config['PUBLISH_PORT'] = args.port
    
    if 'collectors' in config_file:
        config['COLLECTORS'] = config_file['collectors']
    elif os.getenv('COLLECTORS'):
        config['COLLECTORS'] = os.getenv('COLLECTORS')

    if 'log_dir' in config_file:
        configure_logging(config['EXPORTER_LOG_LEVEL'], config_file['log_dir'])
    else:
        configure_logging(config['EXPORTER_LOG_LEVEL'], '/home/azureservicebusexporter/logs')

    for key in config:
        if config[key] is None:
            raise RuntimeError('{0} is not set from config. Exiting.'.format(key))

    return Config(config['AZURE_CLIENT_ID'],
                  config['AZURE_CLIENT_SECRET'],
                  config['AZURE_SUBSCRIPTION_ID'],
                  config['AZURE_TENANT_ID'],
                  config['AZURE_RESOURCE_GROUP'],
                  config['AZURE_NAMESPACE'],
                  config['EXPORTER_LOG_LEVEL'],
                  config['PUBLISH_PORT'],
                  config['COLLECTORS'])

def configure_logging(log_level_str, log_dir):
    if log_level_str == 'critical':
        log_level = logging.CRITICAL
    elif log_level_str == 'error':
        log_level = logging.ERROR
    elif log_level_str == 'warning':
        log_level = logging.WARNING
    elif log_level_str == 'debug':
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    file_name = '{0}/AzureServiceBusExporter.log'.format(log_dir)
    my_formatter = logging.Formatter('[AzureServiceBusExporter] %(asctime)s %(levelname)-7.7s %(message)s')
    my_handler = RotatingFileHandler(file_name, mode='a', maxBytes=5*1024*1024,
                                        backupCount=5, encoding=None, delay=0)
    my_handler.setFormatter(my_formatter)
    my_handler.setLevel(log_level)

    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(my_handler)

    return True