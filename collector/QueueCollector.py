from prometheus_client.core import GaugeMetricFamily
import logging

class QueueCollector(object):
    queue_labels = ('queue_name', 'queue_namespace')

    def __init__(self, client, namespace, resource_group):
        self.logger = logging.getLogger()
        self.client = client
        self.namespace = namespace
        self.resource_group = resource_group
        self.queue_metric_description = self.flush_description()

    def get_queue_metrics(self):
        queues = self.client.list_queues_by_namespace(self.resource_group, self.namespace)
        queue_metrics = [] 
        for queue in queues:
            self.logger.debug('Creating metrics for queue {0}'.format(queue.name))
            metrics = self.create_metrics(queue)
            if metrics:
                self.logger.debug('Appending the following metrics for queue {0}. {1}'.format(queue.name, metrics))
                queue_metrics += metrics
        return queue_metrics

    def flush_description(self):
        description = {}
        description.update({
            'size_in_bytes':                         GaugeMetricFamily('asbe_queue_size_in_bytes', 'Size of the queue in bytes', labels=self.queue_labels),
            'message_count':                         GaugeMetricFamily('asbe_total_queue_message_count', 'Total amount of messages in the queue', labels=self.queue_labels),
            'active_message_count':                  GaugeMetricFamily('asbe_queue_active_message_count', 'Messages that currently being processed', labels=self.queue_labels),
            'dead_letter_message_count':             GaugeMetricFamily('asbe_dead_letter_queue_count', 'Number of messages in the dead-letter queue', labels=self.queue_labels),
            'scheduled_message_count':               GaugeMetricFamily('asbe_scheduled_message_count', 'Messages submitted to a topic for delayed processing', labels=self.queue_labels),
            'transfer_message_count':                GaugeMetricFamily('asbe_transfer_message_count', 'Messages pending transfer into another queue or topic', labels=self.queue_labels),
            'transfer_dead_letter_message_count':    GaugeMetricFamily('asbe_transfer_dead_letter_message_count', 'Messages that failed transfer into another queue or topic and have been moved to transfer dead-letter queue', labels=self.queue_labels)
        })
        return description

    def create_metrics(self, queue):
        metrics = []
        for key in queue._attribute_map.keys():
            if key in self.queue_metric_description.keys():
                self.logger.debug('Current metrics array: {}'.format(metrics))
                val = getattr(queue,key)
                self.logger.info('Updating queue metric {0} with value {1}'.format(self.queue_metric_description[key], val))
                gauge = self.queue_metric_description[key]
                gauge.add_metric([queue.name, self.namespace], val)
                self.logger.debug("Gauge: {}".format(gauge))
                metrics.append(gauge)
            elif key == 'count_details':
                self.logger.debug('Current metrics array (in count details): {}'.format(metrics))
                for attr in getattr(queue,key)._attribute_map.keys():
                    val = getattr(getattr(queue,key), attr)
                    self.logger.info('Updating queue metric {0} with value {1}'.format(self.queue_metric_description[attr], val))
                    gauge = self.queue_metric_description[attr]
                    gauge.add_metric([queue.name, self.namespace], val)
                    self.logger.debug("Adding gauge metric for queue {0}: {1}".format(queue.name, gauge))
                    metrics.append(gauge)
        return metrics

    def collect(self):
        self.queue_metric_description = self.flush_description()
        metrics = self.get_queue_metrics()
        for metric in metrics:
            self.logger.info("All metrics: {}".format(metric))
            yield metric