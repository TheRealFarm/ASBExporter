from azure.mgmt.monitor import MonitorManagementClient
import datetime
import logging

class AzureMonitorClient(object):
    def __init__(self, creds, subscription_id, resource_group, provider, resource_name, base_url=None):
        self.logger = logging.getLogger()
        self.client = MonitorManagementClient(creds, subscription_id, base_url)
        self.subscription_id = subscription_id
        self.provider = provider
        self.resource_id = "/subscriptions/{0}/resourceGroups/{1}/providers/{2}/{3}".format(subscription_id, resource_group, provider, resource_name)

    def set_resource_id(self, resource_id):
        self.resource_id = resource_id
        return self.resource_id

    def set_resource_id_by_parts(self, resource_group, resource_name):
        self.logger.info("Setting new resource id. resource_group: {0}. resource_name: {1}. resource_type/provider: {2}".format(resource_group, resource_name, provider))
        resource_id_string = "/subscriptions/{0}/resourceGroups/{1}/providers/{2}/{3}"
        resource_id = resource_id_string.format(self.subscription_id, resource_group, self.provider, resource_name) 
        self.logger.debug("Full resource string: {0}".format(resource_id))
        self.resource_id = resource_id
        return self.resource_id

    def get_metrics(self, metric_names):
        self.logger.debug("Getting metrics [{0}] for resource_id {1}".format(metric_names, self.resource_id))
        mn = ",".join(metric_names)
        now = datetime.datetime.now() 
        before = now - datetime.timedelta(minutes=5)

        metrics_data = self.client.metrics.list(self.resource_id, timespan="{0}/{1}".format(before, now), interval="PT1M", metricnames=mn, aggregation='Average,Total,Maximum,Minimum,Count')
        self.logger.debug("All metrics data: {0}".format(metrics_data))
        self.logger.debug("All metrics data value: {0}".format(metrics_data.value))
        metrics = {}

        for item in metrics_data.value:
            for ts in item.timeseries:
                for data in reversed(ts.data):
                    self.logger.debug("Appending Metric. Name: {0}, Data: {1}, Unit: {2}".format(item.name.value, data, item.unit.value))
                    metrics[item.name.value] = {'data': data, 'unit': item.unit.value}
        return metrics
        