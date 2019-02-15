from azure.mgmt.servicebus import ServiceBusManagementClient

class ServiceBusClient(object):
    def __init__(self, credentials, subscription_id, base_url=None):
        self.client = ServiceBusManagementClient(credentials, subscription_id, base_url)