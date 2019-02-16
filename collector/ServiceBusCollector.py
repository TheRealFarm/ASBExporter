from client.ServiceBusClient import ServiceBusClient
from prometheus_client.core import GaugeMetricFamily

class ServiceBusCollector(object):
    def __init__(self, client, namespace, samples=5, samplelength=1, collectors=None):
        self.client = client
        self.samples = samples
        self.namespace = namespace
        self.collectors = self.init_collectors(collectors)
        self.samplelength = samplelength

    def init_collectors(self, collectors):
        c = []
        if collectors is None:
            c.append(
                NamespaceCollector(self.client, self.namespace, 'svns-ServiceBus'))
            c.append(
                QueueCollector(self.client, self.namespace, 'svns-ServiceBus'))
        return c 

    def describe(self):
        for collector in self.collectors:
            collector.describe()

    def collect(self):
        for collector in self.collectors:
            metrics = collector.collect()
            print(metrics)
        return None

class NamespaceCollector(object):
    def __init__(self, client, namespace, resource_group):
        self.client = client
        self.namespace = namespace
        self.resource_group = resource_group

    def get_namespace_data(self):
        namespace_data = self.client.get_namespace(self.resource_group, self.namespace)
        return namespace_data

    def describe(self):
        return None

    def collect(self):
        data = self.get_namespace_data()
        return data 

class QueueCollector(object):
    queue_metric_description = {
        'queue_byte_size':          GaugeMetricFamily('queue_size_in_bytes', 'Size of the queue in bytes'),
        'message_count':            GaugeMetricFamily('total_queue_message_count', 'Total amount of messages in the queue'),
        'active_message_count':     GaugeMetricFamily('queue_active_message_count', 'Messages that currently being processed')
    }
    def __init__(self, client, namespace, resource_group):
        self.client = client
        self.namespace = namespace
        self.resource_group = resource_group

    def get_queue_data(self):
        queues = self.client.list_queues_by_namespace(self.resource_group, self.namespace)
        queue_data = [] 
        # I'm not planning on using this list of dictionaries for the data, it was just for testing
        for queue in queues:
            queue_data.append({'name': queue.name})
            queue_data.append({'size_in_bytes': queue.size_in_bytes})
            queue_data.append({'message_count': queue.message_count})
        return queue_data

    def describe(self):
        return None

    def collect(self):
        metrics = self.get_queue_data()
        return metrics