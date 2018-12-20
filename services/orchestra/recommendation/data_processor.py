"""Data processor"""
# pylint: disable=broad-except
import os
import re
import yaml
from framework.log.logger import Logger
from framework.datastore.metric_dao import MockMetricDAO, MockPredictionDAO


class DataProcessor:
    """Data Processor"""
    def __init__(self, logger=None, dao=None, influx_dao=None, config=None):
        self.logger = logger or Logger()
        self.dao = dao or MockMetricDAO()
        self.influx_dao = influx_dao or MockPredictionDAO()

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

    def query_containers_predicted_data(self, namespace, pod_name):
        """Query pod's containers predicted data"""

        try:
            data = dict()
            for metric_type in self.config["metric_types"]:
                queried_data = self.influx_dao.get_container_predicted_data(
                    metric_type=metric_type,
                    namespace=namespace,
                    pod_name=pod_name,
                    duration=self.config.get("data_amount_sec", 7200))
                data.update(
                    {self.config["metric_types"][metric_type]: queried_data})

            data = self._format_containers_workload_data(data)
        except Exception as err:
            self.logger.error("Error in 'query_containers_predicted_data': "
                              "%s", str(err))
            return None

        return data

    def query_pod_observed_data(self, namespace, pod_name):
        """Query pod observed data"""

        containers_data = \
            self.query_containers_observed_data(namespace, pod_name)
        if containers_data is None:
            return None

        try:
            pod_data = dict()
            for metric_type in self.config["metric_types"].values():
                list_points = []
                for container_data in containers_data.values():
                    list_points.append(container_data[metric_type])

                pod_data[metric_type] = \
                    self._sum_up_list_workload_points(list_points)

        except Exception as err:
            self.logger.error("Error in 'query_pod_observed_data': "
                              "%s", str(err))
            return None

        return pod_data

    def query_pod_predicted_data(self, namespace, pod_name):
        """Query pod predicted data"""

        containers_data = \
            self.query_containers_predicted_data(namespace, pod_name)
        if containers_data is None:
            return None

        try:
            pod_data = dict()
            for metric_type in self.config["metric_types"].values():
                list_points = []
                for container_data in containers_data.values():
                    list_points.append(container_data[metric_type])

                pod_data[metric_type] = \
                    self._sum_up_list_workload_points(list_points)
        except Exception as err:
            self.logger.error("Error in 'query_pod_predicted_data': "
                              "%s", str(err))
            return None

        return pod_data

    def query_nodes_observed_data(self):
        """Query node observed data"""

        try:
            data = dict()
            for metric_type in self.config["metric_types"]:
                queried_data = self.dao.get_node_observed_data(
                    metric_type=metric_type,
                    duration=self.config.get("data_amount_sec", 7200))
                data.update(
                    {self.config["metric_types"][metric_type]: queried_data})

            data = self._format_nodes_workload_data(data)
        except Exception as err:
            self.logger.error("Error in 'query_nodes_observed_data': "
                              "%s", str(err))
            return None

        return data

    def query_nodes_predicted_data(self):
        """Query node predicted data"""

        try:
            data = dict()
            for metric_type in self.config["metric_types"]:
                queried_data = self.influx_dao.get_node_predicted_data(
                    metric_type=metric_type,
                    duration=self.config.get("data_amount_sec", 7200))
                data.update(
                    {self.config["metric_types"][metric_type]: queried_data})

            data = self._format_nodes_workload_data(data)
        except Exception as err:
            self.logger.error("Error in 'query_nodes_predicted_data': "
                              "%s", str(err))
            return None

        return data

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

        return self._align_metric_workload_points(formatted_data)

    def _format_nodes_workload_data(self, data):
        """
        Format the queried nodes workload data from
        'by metric type -> by node' to 'by node -> by metric type'
        """

        formatted_data = dict()
        for metric_type, nodes_data in data.items():
            for node_data in nodes_data:
                node_name = node_data["labels"]["node_name"]
                metric_data = self._format_workload_points(
                    node_data["data"], self.config["data_granularity_sec"])

                if node_name not in formatted_data:
                    formatted_data[node_name] = {metric_type: metric_data}
                else:
                    formatted_data[node_name].update(
                        {metric_type: metric_data})

        return self._align_metric_workload_points(formatted_data)

    @staticmethod
    def _format_workload_points(data, time_scaling_sec):
        """Format the workload points to dictionary {TIME: VALUE}."""

        formatted_data = dict()
        for point in data:
            point_timestamp = point["time"] // time_scaling_sec
            point_value = point["value"]
            formatted_data[point_timestamp] = point_value

        return formatted_data

    def _align_metric_workload_points(self, data):
        """
        Align different metric type of workload points by time
        for each container.
        """

        formatted_data = dict()
        for name, sub_data in data.items():
            aligned_data = self._align_list_points(sub_data)
            if not aligned_data:
                return None

            formatted_data[name] = aligned_data

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

    @staticmethod
    def _sum_up_list_workload_points(list_points):
        """Sum up a list of workload points."""

        points_sum = dict()
        for points in list_points:
            points_sum = {k: points_sum.get(k, 0) + points.get(k, 0)
                          for k in set(points_sum) | set(points)}

        return points_sum

    def get_containers_init_resource(self, namespace, pod_name):
        """Get container init_resource"""

        try:
            data = self.dao.get_container_init_resource(
                namespace=namespace,
                pod_name=pod_name)

            data = self._format_containers_requests_limits(data)
        except Exception as err:
            self.logger.error("Error in 'get_containers_init_resource': "
                              "%s %s", type(err), str(err))
            return None

        return data

    def get_containers_resources(self, namespace, pod_name):
        """Get container spec of requests/limits"""

        try:
            data = self.dao.get_container_resources(
                namespace=namespace,
                pod_name=pod_name)

            data = self._format_containers_requests_limits(data)
        except Exception as err:
            self.logger.error("Error in 'get_containers_resources': "
                              "%s %s", type(err), str(err))
            return None

        return data

    def _format_containers_requests_limits(self, data):
        """Format the queried resources to numerical value"""

        formatted_data = dict()
        for container_data in data:
            container_name = container_data["container_name"]
            limits = container_data["resources"]["limits"]
            requests = container_data["resources"]["requests"]

            for metric_type in limits:
                if metric_type == 'cpu':
                    limits[metric_type] = \
                        self._convert_cpu_str2val(limits[metric_type])
                    requests[metric_type] = \
                        self._convert_cpu_str2val(requests[metric_type])
                elif metric_type == 'memory':
                    limits[metric_type] = \
                        self._convert_mem_str2val(limits[metric_type])
                    requests[metric_type] = \
                        self._convert_mem_str2val(requests[metric_type])
                else:
                    raise NameError(metric_type, "Metric type is not defined.")

            formatted_data[container_name] = {
                "limits": limits, "requests": requests}

        return formatted_data

    def _convert_cpu_str2val(self, cpu):
        """Convert the format of cpu resource from string to numerical value"""

        # Check if it can be converted to numerical value in cpu units directly.
        if self._isfloat(cpu):
            return float(cpu)

        # Check if it is in milli core format and then convert to cpu units.
        if cpu.endswith('m') and self._isfloat(cpu.replace('m', '', 1)):
            return float(cpu.replace('m', '')) / 1000.0

        # Otherwise, raise error.
        raise ValueError(cpu, "The format of cpu resource is not supported, "
                         "it cannot be converted to numerical value.")

    def _convert_mem_str2val(self, mem):
        """Convert the format of memory resource from string to
        numerical value"""

        # Check if it can be converted to numerical value in bytes directly.
        if self._isfloat(mem):
            return float(mem)

        # Check if it meets the regular expression with correct memory
        # capacity unit, and convert it to bytes.
        capacity = {
            "K": 1000, "M": 1000 ** 2, "G": 1000 ** 3,
            "T": 1000 ** 4, "P": 1000 ** 5, "E": 1000 ** 6,
            "Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3,
            "Ti": 1024 ** 4, "Pi": 1024 ** 5, "Ei": 1024 ** 6
        }

        regex = '^([0-9.]+)([iEKMGTP]*)$'
        match = re.search(regex, mem)
        if match:
            val, unit = match.groups()
            if self._isfloat(val) and (unit in capacity):
                return float(val) * capacity[unit]

        # Otherwise, raise error.
        raise ValueError(mem, "The format of memory resource is not supported,"
                         " it cannot be converted to numerical value.")

    @staticmethod
    def _isfloat(val):
        """Check if the value can be converted to float or not."""

        try:
            float(val)
        except ValueError:
            return False
        return True

    def write_pod_recommendation_result(self):
        """Write pod recommendation result via gRPC client"""
        pass

    def __format_container_recommendation_result(self):
        """Format the recommendation result to write data by gRPC client."""
        pass
