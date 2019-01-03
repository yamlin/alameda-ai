"""Data processor"""
# pylint: disable=broad-except
import os
import re
import traceback
from datetime import datetime, timedelta
import yaml
from framework.log.logger import Logger
from framework.datastore.metric_dao import MockDAO


class DataProcessor:
    """Data Processor"""

    OBSERVED = "observed"
    PREDICTED = "predicted"
    INIT_RECOMMENDATION = "init_recommendation"
    RECOMMENDATION = "recommendation"
    RESOURCE = "resource"

    def __init__(self, logger=None, dao=None, config=None):
        self.logger = logger or Logger()
        self.dao = dao or MockDAO()

        self.config = config
        if self.config is None:
            app_path = os.path.dirname(os.path.abspath(__file__))
            with open(os.path.join(
                    app_path, 'config/recommendation_conf.yaml')) as file_:
                self.config = yaml.load(file_)

        self.workload_map = {
            self.OBSERVED: {
                "container": "container_metrics",
                "pod": "pod_metrics",
                "node": "node_metrics",
                "metric": "metric_data"},
            self.PREDICTED: {
                "container": "container_predictions",
                "pod": "pod_predictions",
                "node": "node_predictions",
                "metric": "predicted_raw_data"}
        }

        self.resource_map = {
            self.INIT_RECOMMENDATION: {
                "container": "container_recommendations",
                "requests": "initial_request_recommendations",
                "limits": "initial_limit_recommendations"},
            self.RECOMMENDATION: {
                "container": "container_recommendations",
                "requests": "request_recommendations",
                "limits": "limit_recommendations"},
            self.RESOURCE: {
                "container": "containers",
                "requests": "request_resource",
                "limits": "limit_resource"}
        }

    def get_pod_list(self):
        """Get list of pod information that need to predict their workload."""

        pod_list = []
        try:
            queried_data = self.dao.get_data("pod_list", dict())
            pod_list = queried_data["pods"]
        except Exception:
            self.logger.error(traceback.format_exc())

        return pod_list

    def get_node_list(self):
        """Get list of node information that need to predict their workload."""

        node_list = []
        try:
            queried_data = self.dao.get_data("node_list", dict())
            node_list = queried_data["nodes"]
        except Exception:
            self.logger.error(traceback.format_exc())

        return node_list

    def query_containers_init_observed_data(self, pod_info, time_range=None):
        """Query pod's containers initial stage observed data"""

        try:
            data_type = self.OBSERVED
            if not time_range:
                time_range = self.get_time_range(data_type)

            args = {
                "namespaced_name": pod_info["namespaced_name"],
                "time_range": time_range}

            queried_data = self.dao.get_data("container_init", args)
            for pod_metrics in queried_data[self.workload_map[
                    data_type]["pod"]]:
                if pod_metrics["namespaced_name"] == args["namespaced_name"]:
                    data = self._format_containers_workload_data(
                        data_type, pod_metrics)
                    return data

        except Exception:
            self.logger.error(traceback.format_exc())
        return None

    def query_containers_observed_data(self, pod_info, time_range=None):
        """Query pod's containers observed data"""

        try:
            data_type = self.OBSERVED
            if not time_range:
                time_range = self.get_time_range(data_type)

            args = {
                "namespaced_name": pod_info["namespaced_name"],
                "time_range": time_range}

            queried_data = self.dao.get_data("container_observed", args)
            for pod_metrics in queried_data[self.workload_map[
                    data_type]["pod"]]:
                if pod_metrics["namespaced_name"] == args["namespaced_name"]:
                    data = self._format_containers_workload_data(
                        data_type, pod_metrics)
                    return data

        except Exception:
            self.logger.error(traceback.format_exc())
        return None

    def query_containers_predicted_data(self, pod_info, time_range=None):
        """Query pod's containers predicted data"""

        try:
            data_type = self.PREDICTED
            if not time_range:
                time_range = self.get_time_range(data_type)

            args = {
                "namespaced_name": pod_info["namespaced_name"],
                "time_range": time_range}

            queried_data = self.dao.get_data("container_predicted", args)
            for pod_metrics in queried_data[self.workload_map[
                    data_type]["pod"]]:
                if pod_metrics["namespaced_name"] == args["namespaced_name"]:
                    data = self._format_containers_workload_data(
                        data_type, pod_metrics)
                    return data

        except Exception:
            self.logger.error(traceback.format_exc())
        return None

    def query_pod_observed_data(self, pod_info, time_range=None):
        """Query pod observed data"""

        containers_data = self.query_containers_observed_data(
            pod_info, time_range)
        pod_data = self._get_pod_workload_data(containers_data)

        return pod_data

    def query_pod_predicted_data(self, pod_info, time_range=None):
        """Query pod predicted data"""

        containers_data = self.query_containers_predicted_data(
            pod_info, time_range)
        pod_data = self._get_pod_workload_data(containers_data)

        return pod_data

    def _get_pod_workload_data(self, containers_data):
        """Get pod workload data by summing its containers data."""

        try:
            if containers_data:
                # Get all metric types in workload data.
                metric_types = []
                for val in containers_data.values():
                    metric_types += list(val.keys())

                # Sum up containers data with same metric type
                pod_data = dict()
                for metric_type in set(metric_types):
                    list_points = []
                    for container_data in containers_data.values():
                        list_points.append(container_data[metric_type])

                    pod_data[metric_type] = \
                        self._sum_up_list_workload_points(list_points)

                return pod_data

        except Exception:
            self.logger.error(traceback.format_exc())
        return None

    def query_nodes_observed_data(self, node_list, time_range=None):
        """Query node observed data"""

        try:
            data_type = self.OBSERVED
            if not time_range:
                time_range = self.get_time_range(data_type)

            args = {
                "node_name": [node["name"] for node in node_list],
                "time_range": time_range}

            queried_data = self.dao.get_data("node_observed", args)
            data = self._format_nodes_workload_data(data_type,
                                                    queried_data)
            return data

        except Exception:
            self.logger.error(traceback.format_exc())
            return None

    def query_nodes_predicted_data(self, node_list, time_range=None):
        """Query node predicted data"""

        try:
            data_type = self.PREDICTED
            if not time_range:
                time_range = self.get_time_range(data_type)

            args = {
                "node_name": [node["name"] for node in node_list],
                "time_range": time_range}

            queried_data = self.dao.get_data("node_predicted", args)
            data = self._format_nodes_workload_data(data_type,
                                                    queried_data)
            return data

        except Exception:
            self.logger.error(traceback.format_exc())
            return None

    def _format_containers_workload_data(self, data_type, data):
        """
        Format the queried containers workload data from
        'by metric type -> by container' to 'by container -> by metric type'
        """

        formatted_data = dict()
        for container_metrics in data[self.workload_map[
                data_type]["container"]]:
            container_name = container_metrics["name"]
            formatted_data[container_name] = dict()
            for metrics_data in container_metrics[
                    self.workload_map[data_type]["metric"]]:
                metric_type = metrics_data["metric_type"]
                metric_data = self._format_workload_points(
                    metrics_data["data"],
                    self.config["data_granularity_sec"])

                formatted_data[container_name].update(
                    {metric_type: metric_data})

        return self._align_metric_workload_points(formatted_data)

    def _format_nodes_workload_data(self, data_type, data):
        """
        Format the queried nodes workload data from
        'by metric type -> by node' to 'by node -> by metric type'
        """

        formatted_data = dict()
        for node_metrics in data[self.workload_map[data_type]["node"]]:
            node_name = node_metrics["name"]
            formatted_data[node_name] = dict()
            for metrics_data in node_metrics[self.workload_map[
                    data_type]["metric"]]:
                metric_type = metrics_data["metric_type"]
                metric_data = self._format_workload_points(
                    metrics_data["data"],
                    self.config["data_granularity_sec"])

                formatted_data[node_name].update(
                    {metric_type: metric_data})

        return self._align_metric_workload_points(formatted_data)

    def _format_workload_points(self, data, time_scaling_sec):
        """Format the workload points to dictionary {TIME: VALUE}."""

        formatted_data = dict()
        for point in data:
            point_timestamp = \
                self.convert_time(point["time"]) // time_scaling_sec
            point_value = float(point["num_value"])
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

    def get_containers_init_resource(self, pod_info, time_range=None):
        """Get container init_resource"""

        try:
            args = {"namespaced_name": pod_info["namespaced_name"]}
            if time_range:
                args["time_range"] = time_range

            queried_data = self.dao.get_data("container_recommendation", args)
            for pod_metrics in queried_data["pod_recommendations"]:
                if pod_metrics["namespaced_name"] == args["namespaced_name"]:
                    data = self._format_containers_requests_limits(
                        self.INIT_RECOMMENDATION, pod_metrics)
                    return data

        except Exception:
            self.logger.error(traceback.format_exc())
        return None

    def get_containers_resources(self, pod_info):
        """Get container spec of requests/limits"""

        try:
            data = self._format_containers_requests_limits(
                self.RESOURCE, pod_info)
            return data

        except Exception:
            self.logger.error(traceback.format_exc())
        return None

    def _format_containers_requests_limits(self, data_type, data):
        """Format the queried resources to numerical value"""

        formatted_data = dict()
        for container_data in data[self.resource_map[data_type]["container"]]:
            container_name = container_data["name"]

            # requests
            requests = dict()
            requests_data = \
                container_data[self.resource_map[data_type]["requests"]]
            for metric_data in requests_data:
                metric_type = metric_data["metric_type"]
                val = float(metric_data["data"][-1]["num_value"])
                requests[metric_type] = val

            # limits
            limits = dict()
            limits_data = container_data[self.resource_map[data_type]["limits"]]
            for metric_data in limits_data:
                metric_type = metric_data["metric_type"]
                val = float(metric_data["data"][-1]["num_value"])
                limits[metric_type] = val

            formatted_data[container_name] = {
                "limits": limits, "requests": requests}

        return formatted_data

    @staticmethod
    def convert_time(time):
        """Convert string to integer or convert integer/datetime to
        string format."""

        epoch = datetime(1970, 1, 1)
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        if isinstance(time, str):
            converted_time = datetime.strptime(time, time_format)
            converted_time = int((converted_time - epoch).total_seconds())
        elif isinstance(time, datetime):
            converted_time = int((time - epoch).total_seconds())
            converted_time = \
                datetime.utcfromtimestamp(converted_time).strftime(time_format)
        elif isinstance(time, int):
            converted_time = \
                datetime.utcfromtimestamp(time).strftime(time_format)
        else:
            raise TypeError(time, "The type of time object is not string "
                                  "nor integer while trying to convert "
                                  "time object.")

        return converted_time

    def get_time_range(self, data_type, time=None, duration=None):
        """Format time range with timestamp and duration."""

        if not time:  # current time if there's no requested time
            time = datetime.utcnow()
        if not duration:  # default data amount if there's no requested duration
            duration = \
                timedelta(seconds=self.config.get("data_amount_sec", 7200))

        if data_type is self.PREDICTED:
            time_range = {
                "start_time": self.convert_time(time),
                "end_time": self.convert_time(time + duration),
                "step": self.config.get("data_granularity_sec", 30)
            }
        else:
            time_range = {
                "start_time": self.convert_time(time - duration),
                "end_time": self.convert_time(time),
                "step": self.config.get("data_granularity_sec", 30)
            }

        return time_range

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

    def get_pod_recommendation_result(self, pod, init_resource, resources):
        """Write pod recommendation result via gRPC client"""

        try:
            result = {
                "uid": pod["uid"],
                "namespace": pod["namespace"],
                "pod_name": pod["pod_name"],
                "containers": self._format_container_recommendation_result(
                    init_resource, resources)
            }
            return result
        except Exception as err:
            self.logger.error("Error in 'write_pod_recommendation_result': "
                              "%s %s", type(err), str(err))

    def _format_container_recommendation_result(self, init_resource, resources):
        """Format the recommendation result to write data by gRPC client."""

        formatted_data = []
        time_scaling_sec = self.config["data_granularity_sec"]
        for container_name in resources:
            container_set = {
                "container_name": container_name,
                "recommendations": []
            }
            for resource_set in resources[container_name]:

                recommendation_set = {
                    "time": resource_set["time"] * time_scaling_sec,
                    "resources": {
                        "requests": resource_set["requests"],
                        "limits": resource_set["limits"]
                    }
                }
                container_set["recommendations"].append(recommendation_set)

            if init_resource and init_resource.get(container_name):
                container_set["init_resource"] = init_resource[container_name]

            formatted_data.append(container_set)

        return formatted_data
