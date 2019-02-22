import time
import requests
import argparse
import os
import adal
import random
import yaml
import logging
from logging.handlers import RotatingFileHandler

from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY
from collector import ServiceBusCollector
from client import ServiceBusClient
from msrestazure.azure_active_directory import AADTokenCredentials

def main(creds, subscription_id, namespace, resource_group):
	logger = logging.getLogger()
    sb_client = ServiceBusClient.ServiceBusClient(creds, subscription_id)
    collector = ServiceBusCollector.ServiceBusCollector(sb_client, namespace, creds, resource_group, subscription_id) 
    REGISTRY.register(collector)
    start_http_server(9145)

    while True:
        time.sleep(60)

def get_credentials(tenant_id, client_id, client_secret):
    authority_uri = '{0}/{1}'.format('https://login.microsoftonline.com', tenant_id)
    api_uri = 'https://management.core.windows.net'
    context = adal.AuthenticationContext(authority_uri, api_version=None)
    token = context.acquire_token_with_client_credentials(api_uri, client_id, client_secret)
    creds = AADTokenCredentials(token, client_id)
    if not creds:
        raise RuntimeError("Could not obtain credentials")
    return creds

def configure_logging(script_dir, log_file):
    abs_path = os.path.join(script_dir, log_file)
    with open(abs_path, 'r') as yamlfile:
        log_config = yaml.load(yamlfile)

    if 'log_dir' in log_config:
        log_dir = log_config['log_dir']
    else:
        log_dir = script_dir

    if 'log_level' in log_config:
        log_conf_level = log_config['log_level']
    else:
        log_conf_level = 'info'

    if log_conf_level == 'critical':
        log_level = logging.CRITICAL
    elif log_conf_level == 'error':
        log_level = logging.ERROR
    elif log_conf_level == 'warning':
        log_level = logging.WARNING
    elif log_conf_level == 'debug':
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--client_id', '-c', required=False, help='Client ID for Azure AD Application')
    parser.add_argument('--client-secret' '-x', required=False, help='Client Secret for Azure AD Application')
    parser.add_argument('--subscription_id', '-s', required=False,help='Azure Subscripotion your resources reside in.')
    parser.add_argument('--tenant_id', '-t', required=False, help='Azure Tenant Id.')
    parser.add_argument('--resource_group', '-g', required=False, help='Resource group that the Service Bus namespace resides in')
    parser.add_argument('--namespace', '-n', required=False, help='Namespace for the Service Bus instance')

    if not os.getenv('APP_CLIENT_SECRET'):
        raise RuntimeError('Client Secret not set')
    client_secret = os.getenv('APP_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    client_id = os.getenv('AZURE_CLIENT_ID')
    creds = get_credentials(tenant_id, client_id, client_secret)
    subscription_id = os.getenv('AZURE_SUBSCRIPTION_ID')
    resource_group = 'pusw-ServiceBus'
    namespace = 'puswservicebus'

    main(creds, subscription_id, namespace, resource_group)