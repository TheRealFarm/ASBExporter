from azure.mgmt.servicebus import ServiceBusManagementClient

class ServiceBusClient(object):
    def __init__(self, credentials, subscription_id, base_url=None):
        self.client = ServiceBusManagementClient(credentials, subscription_id, base_url)

    def list_all_namespaces(self):
        return self.client.namespaces.list()

    def list_namespace_by_resource_group(self, resource_group):
        return self.client.namespaces.list_by_resource_group(resource_group)

    def get_namespace(self, resource_group, namespace):
        return self.client.namespaces.get(resource_group, namespace)

    def list_queues_by_namespace(self, resource_group, namespace):
        return self.client.queues.list_by_namespace(resource_group, namespace)

    def get_queue(self, resource_group, namespace, queue_name):
        return self.client.queues.get(resource_group, namespace, queue_name)