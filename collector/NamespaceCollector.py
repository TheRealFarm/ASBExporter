from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from azure.mgmt.monitor.models import ErrorResponseException
import logging

class NamespaceCollector(object):
    namespace_labels = ['namespace']
    supported_aggregations = ['average', 'minimum', 'maximum', 'total', 'count']
    premium_metrics_list = ['CPUXNS','WSXNS']

    def __init__(self, client, namespace, resource_group, premium=False):
        self.logger = logging.getLogger()
        self.client = client
        self.namespace = namespace
        self.resource_group = resource_group
        self.premium = premium
        self.namespace_metric_description = self.flush_description(premium)
    
    def get_namespace_metrics(self):
        namespace_metrics = []
        namespace_metric_dict = {}
        namespace_metric_dict.update(self.client.get_metrics(list(self.namespace_metric_description.keys())))
        try:
            # try to request premium metrics from the azure api for this namespace
            premium = self.client.get_metrics(self.premium_metrics_list)
            namespace_metric_dict.update(premium)
            self.premium = True
        except ErrorResponseException:
            self.logger.error('{0} is not a premium namespace. Will not gather cpu/memory usage metrics.'.format(self.namespace))
        metrics = self.create_metrics(namespace_metric_dict)
        if metrics:
            namespace_metrics += metrics
        return namespace_metrics

    def flush_description(self, premium=False):
        description = {}
        if premium:
            description.update({
                'CPUXNS':  GaugeMetricFamily('asbe_namespace_cpu_usage', 'CPU usage per namespace', labels=self.namespace_labels),
                'WSXNS':   GaugeMetricFamily('asbe_namespace_memory_usage', 'Memory size usage per namespace', labels=self.namespace_labels)
            })

        description.update({
            'ActiveConnections':  GaugeMetricFamily('asbe_namespace_active_connections', 'Number of active connections on a namespace', labels=self.namespace_labels),
            'ServerErrors':       CounterMetricFamily('asbe_namespace_server_errors_count', 'Number of requests not processed due to an error in the Service Bus service', labels=self.namespace_labels),
            'UserErrors':         CounterMetricFamily('asbe_namespace_user_errors_count', 'Number of requests not processed due to user errors over a specified period', labels=self.namespace_labels),
            'ThrottledRequests':  CounterMetricFamily('asbe_namespace_throttled_requests_count', 'Number of requests throttled due to exceeding usage', labels=self.namespace_labels),
            'IncomingRequests':   CounterMetricFamily('asbe_namespace_incoming_requests_count', 'Number of requests made to the Service Bus service', labels=self.namespace_labels),
            'SuccessfulRequests': CounterMetricFamily('asbe_namespace_successful_requests_count', 'Number of successful requests made to the Service Bus service', labels=self.namespace_labels),
            'IncomingMessages':   GaugeMetricFamily('asbe_namespace_incoming_messages_count', 'Number of events or messages sent to Service Bus', labels=self.namespace_labels),
            'OutgoingMessages':   GaugeMetricFamily('asbe_namespace_outgoing_messages_count', 'Number of events or messages received from Service Bus', labels=self.namespace_labels)
        })
        return description


    def create_metrics(self, namespace_metric_dict):
        metrics = []
        for key in namespace_metric_dict:
            gauge = self.namespace_metric_description[key]
            data = namespace_metric_dict[key]['data'] 
            unit = namespace_metric_dict[key]['unit']
            if unit.lower() not in self.supported_aggregations: # this means its most likely a 'Percent'
                unit = 'average'
            val = getattr(data, unit.lower()) 
            self.logger.debug('Gauge: {}'.format(gauge))
            gauge.add_metric([self.namespace], val)
            self.logger.info('Adding gauge metric for namespace {0}: {1}'.format(self.namepsace, gauge))
            metrics.append(gauge)
        return metrics

    def collect(self):
        self.namespace_metric_description = self.flush_description(self.premium)
        metrics = self.get_namespace_metrics()
        for metric in metrics:
            self.logger.debug('Yielding metric: {}'.format(metric))
            yield metric