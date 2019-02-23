from client import AzureMonitorClient
from prometheus_client.core import GaugeMetricFamily
from collector import QueueCollector, NamespaceCollector
import logging

class ServiceBusCollector(object):
    def __init__(self, client, namespace, creds, resource_group, subscription_id, samples=5, samplelength=1, collectors=None):
        self.logger = logging.getLogger()
        self.client = client
        self.samples = samples
        self.namespace = namespace
        self.samplelength = samplelength
        self.creds = creds
        self.resource_group = resource_group
        self.subscription_id = subscription_id
        self.collectors = self.init_collectors(collectors)

    def init_collectors(self, collectors):
        c = []
        if collectors is None:
            monitor_client = AzureMonitorClient.AzureMonitorClient(self.creds, self.subscription_id, self.resource_group, 'Microsoft.ServiceBus/namespaces', self.namespace)
            self.logger.debug('Registering NamespaceCollector with the following parameters. Namespace={0}, Resource group={1}'.format(self.namespace, self.resource_group))
            c.append(
                NamespaceCollector.NamespaceCollector(monitor_client, self.namespace, self.resource_group)) 
            self.logger.debug('Registering QueueCollector with the following parameters. Namespace={0}, Resource group={1}'.format(self.namespace, self.resource_group))
            c.append(
                QueueCollector.QueueCollector(self.client, self.namespace, self.resource_group))
        return c

    def collect(self):
        for collector in self.collectors:
            metrics = collector.collect()
            if metrics:
               for metric in metrics:
                   yield metric