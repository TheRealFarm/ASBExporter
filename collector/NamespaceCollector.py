from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from azure.mgmt.monitor.models import ErrorResponseException

class NamespaceCollector(object):
    namespace_labels = ('namespace')
    namespace_metric_description = {
        'ActiveConnections':  GaugeMetricFamily('namespace_active_connections', 'Number of active connections on a namespace', labels=namespace_labels),
        'ServerErrors':       CounterMetricFamily('namespace_server_errors_count', 'Number of requests not processed due to an error in the Service Bus service', labels=namespace_labels),
        'UserErrors':         CounterMetricFamily('namespace_user_errors_count', 'Number of requests not processed due to user errors over a specified period', labels=namespace_labels),
        'ThrottledRequests':  CounterMetricFamily('namespace_throttled_requests_count', 'Number of requests throttled due to exceeding usage', labels=namespace_labels),
        'IncomingRequests':   CounterMetricFamily('namespace_incoming_requests_count', 'Number of requests made to the Service Bus service', labels=namespace_labels),
        'SuccessfulRequests': CounterMetricFamily('namespace_successful_requests_count', 'Number of successful requests made to the Service Bus service', labels=namespace_labels),
        'IncomingMessages':   GaugeMetricFamily('namespace_incoming_messages_count', 'Number of events or messages sent to Service Bus', labels=namespace_labels),
        'OutgoingMessages':   GaugeMetricFamily('namespace_outgoing_messages_count', 'Number of events or messages received from Service Bus', labels=namespace_labels)
    }
    premium_namespace_metric_description = {
        'CPUXNS':  GaugeMetricFamily('namespace_cpu_usage', 'CPU usage per namespace', labels=namespace_labels),
        'WSXNS':   GaugeMetricFamily('namespace_memory_usage', 'Memory size usage per namespace', labels=namespace_labels)
    }
    supported_aggregations = ['average', 'minimum', 'maximum', 'total', 'count']

    def __init__(self, client, namespace, resource_group, premium=False):
        self.client = client
        self.namespace = namespace
        self.resource_group = resource_group
    
    def get_namespace_metrics(self):
        namespace_metrics = []
        namespace_metric_dict = {}
        namespace_metric_dict.update(self.client.get_metrics(list(self.namespace_metric_description.keys())))
        try:
            premium = self.client.get_metrics(list(self.premium_namespace_metric_description.keys()))
            namespace_metric_dict.update(premium)
        except ErrorResponseException:
            print("Namespace is not premium")
        metrics = self.create_metrics(namespace_metric_dict)
        if metrics:
            namespace_metrics += metrics
        return namespace_metrics

    def create_metrics(self, namespace_metric_dict):
        metrics = []
        for key in namespace_metric_dict:
            if key not in self.namespace_metric_description:
                gauge = self.premium_namespace_metric_description[key]
            else:
                gauge = self.namespace_metric_description[key]
            data = namespace_metric_dict[key]['data'] 
            unit = namespace_metric_dict[key]['unit']
            if unit.lower() not in self.supported_aggregations: # this means its most likely a 'Percent'
                unit = 'average'
            val = getattr(data, unit.lower()) 
            gauge.add_metric([self.namespace], val)
            metrics.append(gauge)
        return metrics

    def collect(self):
        return self.get_namespace_metrics() 