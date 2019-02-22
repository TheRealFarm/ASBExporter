from client import AzureMonitorClient
from prometheus_client.core import GaugeMetricFamily
from collector import QueueCollector, NamespaceCollector

class ServiceBusCollector(object):
    def __init__(self, client, namespace, creds, resource_group, subscription_id, samples=5, samplelength=1, collectors=None, premium=False):
        self.client = client
        self.samples = samples
        self.namespace = namespace
        self.premium = premium
        self.samplelength = samplelength
        self.creds = creds
        self.resource_group = resource_group
        self.subscription_id = subscription_id
        self.collectors = self.init_collectors(collectors)

    def init_collectors(self, collectors):
        c = []
        if collectors is None:
            monitor_client = AzureMonitorClient.AzureMonitorClient(self.creds, self.subscription_id, self.resource_group, 'Microsoft.ServiceBus/namespaces', self.namespace)
            c.append(
                NamespaceCollector.NamespaceCollector(monitor_client, self.namespace, self.resource_group, self.premium)) 
            c.append(
                QueueCollector.QueueCollector(self.client, self.namespace, self.resource_group))
        return c

    def collect(self):
        for collector in self.collectors:
            metrics = collector.collect()
            if metrics:
               for metric in metrics:
                   yield metric