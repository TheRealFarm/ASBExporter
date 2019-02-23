import time
import requests
import argparse
import os
import adal
import random
import sys
import logging
from logging.handlers import RotatingFileHandler

from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY
from collector import ServiceBusCollector
from client import ServiceBusClient
from msrestazure.azure_active_directory import AADTokenCredentials
from config import init_config

def main(creds, config):
    logger = logging.getLogger()
    sb_client = ServiceBusClient.ServiceBusClient(creds, config.subscription_id)
    collector = ServiceBusCollector.ServiceBusCollector(sb_client, config.namespace, creds, config.resource_group, config.subscription_id) 
    try:
        REGISTRY.register(collector)
    except Exception as e:
        logger.error('Error registering the collectors. {0}'.format(e))
        sys.exit(1)
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--client_id', '-c', required=False, help='Client ID for Azure AD Application')
    parser.add_argument('--client_secret' '-x', required=False, help='Client Secret for Azure AD Application')
    parser.add_argument('--subscription_id', '-s', required=False,help='Azure Subscripotion your resources reside in.')
    parser.add_argument('--tenant_id', '-t', required=False, help='Azure Tenant Id.')
    parser.add_argument('--resource_group', '-g', required=False, help='Resource group that the Service Bus namespace resides in')
    parser.add_argument('--namespace', '-n', required=False, help='Namespace for the Service Bus instance')
    parser.add_argument('--port', '-p', required=False, help='Port for the exporter to expose metrics on')
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.realpath(__file__))
    config = init_config(script_dir, args)

    creds = get_credentials(config.tenant_id, config.client_id, config.client_secret)
    main(creds, config)