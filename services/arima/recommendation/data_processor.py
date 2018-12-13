"""Data processor"""
# pylint: disable=broad-except
import os
import yaml
from framework.log.logger import Logger
from framework.datastore.metric_dao import MockMetricDAO


class DataProcessor:
    """Data Processor"""
    def __init__(self, logger=None, dao=None, config=None):
        self.logger = logger or Logger()
        self.dao = dao or MockMetricDAO()

        self.config = config
        if self.config is None:
            app_path = os.path.dirname(os.path.abspath(__file__))
            with open(os.path.join(
                    app_path, 'config/recommendation_conf.yaml')) as file_:
                self.config = yaml.load(file_)

    def query_containers_init_observed_data(self, namespace, pod_name):
        """Query pod's containers initial stage observed data"""

        try:
            data = dict()
            for metric_type in self.config["metric_types"]:
                queried_data = self.dao.get_container_init_data(
                    metric_type=metric_type,
                    namespace=namespace,
                    pod_name=pod_name,
                    duration=self.config.get("data_amount_init_sec", 300))
                data.update(
                    {self.config["metric_types"][metric_type]: queried_data})

            data = self._format_containers_workload_data(data)
        except Exception as err:
            self.logger.error("Error in 'query_containers_init_observed_data': "
                              "%s", str(err))
            return None

        return data

    def query_containers_observed_data(self, namespace, pod_name):
        """Query pod's containers observed data"""

        try:
            data = dict()
            for metric_type in self.config["metric_types"]:
                queried_data = self.dao.get_container_observed_data(
                    metric_type=metric_type,
                    namespace=namespace,
                    pod_name=pod_name,
                    duration=self.config.get("data_amount_sec", 7200))
                data.update(
                    {self.config["metric_types"][metric_type]: queried_data})

            data = self._format_containers_workload_data(data)
        except Exception as err:
            self.logger.error("Error in 'query_containers_observed_data': "
                              "%s", str(err))
            return None

        return data

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

    def _format_containers_workload_data(self, data):
        """
        Format the queried containers workload data from
        'by metric type -> by container' to 'by container -> by metric type'
        """

        formatted_data = dict()
        for metric_type, containers_data in data.items():
            for container_data in containers_data:
                container_name = container_data["labels"]["container_name"]
                metric_data = self._format_workload_points(
                    container_data["data"], self.config["data_granularity_sec"])

                if container_name not in formatted_data:
                    formatted_data[container_name] = {metric_type: metric_data}
                else:
                    formatted_data[container_name].update(
                        {metric_type: metric_data})

        return self._align_container_metric_workload_points(formatted_data)

    @staticmethod
    def _format_workload_points(data, time_scaling_sec):
        """Format the workload points to dictionary {TIME: VALUE}."""

        formatted_data = dict()
        for point in data:
            point_timestamp = point["time"] // time_scaling_sec
            point_value = point["value"]
            formatted_data[point_timestamp] = point_value

        return formatted_data

    def _align_container_metric_workload_points(self, data):
        """
        Align different metric type of workload points by time
        for each container.
        """

        formatted_data = dict()
        for container_name, container_data in data.items():
            aligned_data = self._align_list_points(container_data)
            if not aligned_data:
                return None

            formatted_data[container_name] = aligned_data

        return formatted_data

    @staticmethod
    def _align_list_points(points_map):
        """
        Align a list of workload points by time.
        :param points_map: (dict) list of points with corresponding key name
        """

        # Find intersection of time in points_map
        points_match = []
        for points in points_map.values():
            if not points_match:
                points_match = list(points.keys())
            else:
                points_match = list(set(points_match) & set(points.keys()))
        if not points_match:
            return None

        # Align every set of points in points_map
        points_map_matched = dict()
        for key, points in points_map.items():
            points_map_matched[key] = {k: points[k] for k in points_match}
        return points_map_matched

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
