import time
import requests
import argparse
import os
import adal

from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY
from collector import ServiceBusCollector
from client import ServiceBusClient
from msrestazure.azure_active_directory import AADTokenCredentials

def main(creds, subscription_id):
    sb_client = ServiceBusClient.ServiceBusClient(creds, subscription_id)
    sb_client.client.queues.list_by_namespace('svns-ServiceBus', 'svnsservicebus')

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
    parser.add_argument('--client-secret' '-x', required=False, help='Client Secret for Azure AD Application')
    parser.add_argument('--subscription_id', '-s', required=False,help='Azure Subscripotion your resources reside in.')
    parser.add_argument('--tenant_id', '-t', required=False, help='Azure Tenant Id.')
    if not os.getenv('APP_CLIENT_SECRET'):
        raise RuntimeError('Client Secret not set')
    client_secret = os.getenv('APP_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    client_id = os.getenv('AZURE_CLIENT_ID')
    creds = get_credentials(tenant_id, client_id, client_secret)
    subscription_id = os.getenv('AZURE_SUBSCRIPTION_ID')

    main(creds, subscription_id)