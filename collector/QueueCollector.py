from prometheus_client.core import GaugeMetricFamily

class QueueCollector(object):
    queue_labels = ('queue_name', 'queue_namespace')
    queue_metric_description = {
        'size_in_bytes':                         GaugeMetricFamily('queue_size_in_bytes', 'Size of the queue in bytes', labels=queue_labels),
        'message_count':                         GaugeMetricFamily('total_queue_message_count', 'Total amount of messages in the queue', labels=queue_labels),
        'active_message_count':                  GaugeMetricFamily('queue_active_message_count', 'Messages that currently being processed', labels=queue_labels),
        'dead_letter_message_count':             GaugeMetricFamily('dead_letter_queue_count', 'Number of messages in the dead-letter queue', labels=queue_labels),
        'scheduled_message_count':               GaugeMetricFamily('scheduled_message_count', 'Messages submitted to a topic for delayed processing', labels=queue_labels),
        'transfer_message_count':                GaugeMetricFamily('transfer_message_count', 'Messages pending transfer into another queue or topic', labels=queue_labels),
        'transfer_dead_letter_message_count':    GaugeMetricFamily('transfer_dead_letter_message_count', 'Messages that failed transfer into another queue or topic and have been moved to transfer dead-letter queue', labels=queue_labels)
    }

    def __init__(self, client, namespace, resource_group):
        self.client = client
        self.namespace = namespace
        self.resource_group = resource_group

    def get_queue_metrics(self):
        queues = self.client.list_queues_by_namespace(self.resource_group, self.namespace)
        queue_metrics = [] 
        for queue in queues:
            metrics = self.create_metrics(queue)
            if metrics:
                queue_metrics += metrics
        return queue_metrics

    def create_metrics(self, queue):
        metrics = []
        for key in queue._attribute_map.keys():
            if key in self.queue_metric_description.keys():
                gauge = self.queue_metric_description[key]
                gauge.add_metric([queue.name, self.namespace], getattr(queue, key))
                metrics.append(gauge)
            elif key == 'count_details':
                for attr in getattr(queue,key)._attribute_map.keys():
                    gauge = self.queue_metric_description[attr]
                    gauge.add_metric([queue.name, self.namespace], getattr(getattr(queue,key), attr))
                    metrics.append(gauge)
        return metrics

    def collect(self):
        return self.get_queue_metrics()