"""Data processor"""

from framework.log.logger import Logger
from framework.datastore.metric_dao import MockMetricDAO


class DataProcessor:
    """Data Processor"""
    def __init__(self, logger=None, dao=None):
        self.logger = logger or Logger()
        self.dao = dao or MockMetricDAO()

    def query_containers_init_observed_data(self):
        """Query pod's containers initial stage observed data"""
        pass

    def query_containers_observed_data(self):
        """Query pod's containers observed data"""
        # Query from Prometheus via gRPC
        pass

    def query_containers_predicted_data(self):
        """Query pod's containers predicted data"""
        # Query from internal Influxdb
        pass

    def query_pod_observed_data(self):
        """Query pod observed data"""
        pass

    def query_pod_predicted_data(self):
        """Query pod predicted data"""
        pass

    def query_nodes_observed_data(self):
        """Query node observed data"""
        pass

    def query_nodes_predicted_data(self):
        """Query node predicted data"""
        pass

    def __format_workload_data(self):
        """Format the queried workload data"""
        pass

    def get_container_init_resource(self):
        """Get container init_resource"""
        pass

    def get_container_resources(self):
        """Get container spec of requests/limits"""
        pass

    def __format_requests_limits(self):
        """Format the queried resources to numerical value"""
        pass

    def write_pod_recommendation_result(self):
        """Write pod recommendation result via gRPC client"""
        pass

    def __format_container_recommendation_result(self):
        """Format the recommendation result to write data by gRPC client."""
        pass
