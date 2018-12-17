# -*- coding: utf-8 -*-
""" Recommendation """
# pylint: disable=E0401
import math
import numpy as np

from framework.log.logger import Logger, LogLevel
from framework.datastore.metric_dao import MetricDAO
from services.orchestra.workload_prediction.workload_utils import \
    get_csv_data, get_container_name, get_metric_name_and_conf


class Metric:
    """Metric"""
    CPU = "cpu"
    MEM = "memory"

    @classmethod
    def has_val(cls, value):
        """Has value in the class"""
        return any(value == v for v in vars(cls).values())


class Policy:
    """Policy"""
    STABLE = "STABLE"
    COMPACT = "COMPACT"

    @classmethod
    def has_val(cls, value):
        """Has value in the class"""
        return any(value == v for v in vars(cls).values())


class Recommender:
    """Recommender"""

    def __init__(self, measurement_conf, log=None, dao=None):
        self.time_scaling_sec = 1
        self.init_stage_duration_sec = 3 * 60
        self.tolerance = 1.2
        self.prdt_tolerance = 1.2
        self.policy_partition = 3
        self.policy = Policy.STABLE

        self.measurement_conf = measurement_conf

        self.log = log or Logger(name='Recommender',
                                 logfile='/var/log/recommender.log',
                                 level=LogLevel.LV_DEBUG)
        self.dao = dao or MetricDAO()

    def set_time_scaling_sec(self, time_scaling_sec):
        """Set time_scaling_sec"""
        self.time_scaling_sec = time_scaling_sec

    def set_policy(self, policy):
        """Set time_scaling_sec"""
        self.policy = policy

    def recommend(self, pod_info, output_file_list, filename_tags_map):
        """Get containers recommendation result from specific pod."""

        # [1.1] Get pod policy.
        policy = pod_info.get("policy")
        if Policy.has_val(policy):
            self.set_policy(policy)
        else:
            self.log.error("[Recommender] Policy \"%s\" "
                           "is not supported", policy)
            return

        container_set = dict()
        container_init_set = dict()
        for prdt_file_path in output_file_list:

            # [1.2] Get corresponding container name and metric name.
            container_name = get_container_name(prdt_file_path, filename_tags_map)
            metric_name, _ = get_metric_name_and_conf(
                prdt_file_path, self.measurement_conf)

            if container_name is None or metric_name is None:
                continue
            if not Metric.has_val(metric_name):
                self.log.warning("[Recommender] Metric \"%s\" "
                                 "are not supported", metric_name)
                continue

            # [2] Compute requests/limits from observed/prediction data.
            file_path = prdt_file_path.replace('output', 'input')
            file_path = file_path.replace('.prdt', '')
            is_exist, requests, limits = \
                self.init_stage(file_path, prdt_file_path, metric_name)
            if is_exist:
                init_resource, resource_set = self.prediction_stage(
                    prdt_file_path, metric_name, requests, limits)

                if container_name not in container_init_set:
                    container_init_set[container_name] = init_resource
                    container_set[container_name] = resource_set
                else:
                    container_init_set[container_name].update(init_resource)
                    container_set[container_name].update(resource_set)

        # [3] Format the pod recommendation result again.
        self.write_recommendation_result(pod_info,
                                         container_init_set, container_set)

    def init_stage(self, file_path, prdt_file_path, metric_name):
        """Initial stage requests/limits"""
        is_exist = False
        requests = 0
        limits = 0

        data = get_csv_data(prdt_file_path)
        num_sample, _ = data.shape

        # When this container is running more than specific time,
        # recommender would start to compute recommended resources.
        num_sample_needed = \
            self.init_stage_duration_sec // self.time_scaling_sec
        if num_sample >= num_sample_needed:
            is_exist = True

            data = get_csv_data(file_path)
            values = data[:, 1]

            requests, limits = self.__compute_requests_limits(
                metric_name, values, self.tolerance)

        return is_exist, requests, limits

    def prediction_stage(self, file_path, metric_name, requests, limits):
        """Prediction stage requests/limits"""
        list_resource_set = []

        data = get_csv_data(file_path)
        times = data[:, 0]
        values = data[:, 1]

        prdt_requests, prdt_limits = self.__compute_requests_limits(
            metric_name, values, self.prdt_tolerance)
        timestamp = int(times[0] * self.time_scaling_sec)
        prdt_requests = max(requests, prdt_requests)
        prdt_limits = max(limits, prdt_limits)

        init_resource = self.format_container_resources(
            metric_name, timestamp, prdt_requests, prdt_limits)

        if self.policy == Policy.COMPACT:
            times = np.array_split(times, self.policy_partition)
            values = np.array_split(values, self.policy_partition)

            for time, val in zip(times, values):
                prdt_requests, prdt_limits = self.__compute_requests_limits(
                    metric_name, val, self.prdt_tolerance)
                prdt_requests = max(requests, prdt_requests)
                prdt_limits = max(limits, prdt_limits)

                timestamp = int(time[0] * self.time_scaling_sec)
                resource_set = self.format_container_resources(
                    metric_name, timestamp, prdt_requests, prdt_limits)
                list_resource_set.append(resource_set)
        else:
            resource_set = self.format_container_resources(
                metric_name, timestamp, prdt_requests, prdt_limits)
            list_resource_set.append(resource_set)

        return init_resource, {metric_name: list_resource_set}

    def format_container_resources(self, metric_name, timestamp, requests, limits):
        """Format container recommended resources to meet the spec of k8s"""
        resource_set = {}
        if metric_name == Metric.MEM:
            resource_set = {
                metric_name: {
                    "time": timestamp,
                    "requests": self.__convert_memory_unit(requests),
                    "limits": self.__convert_memory_unit(limits)
                }
            }
        elif metric_name == Metric.CPU:
            resource_set = {
                metric_name: {
                    "time": timestamp,
                    "requests": self.__convert_cpu_unit(requests),
                    "limits": self.__convert_cpu_unit(limits)
                }
            }

        return resource_set

    def write_recommendation_result(self, pod_info, container_init_set, container_set):
        """Write recommendation result via gRPC client"""

        container_result = self.integrate_container_recommendation_result(
            container_init_set, container_set)
        if container_result:
            out_data = {"uid": pod_info.get("uid"),
                        "namespace": pod_info.get("namespace"),
                        "pod_name": pod_info.get("pod_name"),
                        "containers": list(container_result.values())}
            self.log.debug("Write recommendation data: %s", out_data)
            try:
                self.dao.write_container_recommendation_result(out_data)
            except Exception as err:  # pylint: disable=W0703
                self.log.error(err)
                self.log.error("Write POD recommendation error: {%s}",
                               pod_info)

    def integrate_container_recommendation_result(self, container_init_set, container_set):
        """Integrate init_resource and recommendation result"""
        container_init_result = \
            self.format_container_init_resource(container_init_set)
        container_result = \
            self.format_container_recommendation_result(container_set)

        for container_name in container_init_result:
            if container_name in container_result:
                container_result[container_name].update(
                    container_init_result[container_name])
            else:
                container_result[container_name] = \
                    container_init_result[container_name]

        return container_result

    @staticmethod
    def format_container_init_resource(container_set):
        """Format the init_resource result to write data by GRPC client."""
        container_recommendation_set = dict()
        for container_name in container_set:
            init_set = {
                "limits": dict(),
                "requests": dict()
            }

            for metric_name, metric_set in \
                    container_set[container_name].items():

                init_set["limits"].update(
                    {metric_name: metric_set["limits"]})
                init_set["requests"].update(
                    {metric_name: metric_set["requests"]})

            container_recommendation_set[container_name] = {
                "container_name": container_name,
                "init_resource": init_set
            }

        return container_recommendation_set

    @staticmethod
    def format_container_recommendation_result(container_set):
        """Format the recommendation result to write data by GRPC client."""
        container_recommendation_set = dict()
        for container_name in container_set:
            recommendation_time_dict = {}
            for metric_name, list_resource_set in \
                    container_set[container_name].items():

                for metric_set in list_resource_set:
                    metric_set = metric_set[metric_name]
                    timestamp = metric_set["time"]
                    recommendation_set = {
                        "time": timestamp,
                        "resources": {
                            "limits": {metric_name: metric_set["limits"]},
                            "requests": {metric_name: metric_set["requests"]}
                        }
                    }

                    if timestamp not in recommendation_time_dict:
                        recommendation_time_dict[timestamp] = recommendation_set
                    else:
                        recommendation_time_dict[timestamp][
                            "resources"]["limits"].update(
                                recommendation_set["resources"]["limits"])
                        recommendation_time_dict[timestamp][
                            "resources"]["requests"].update(
                                recommendation_set["resources"]["requests"])

            container_recommendation_set[container_name] = {
                "container_name": container_name,
                "recommendations": list(recommendation_time_dict.values())
            }

        return container_recommendation_set

    @staticmethod
    def __compute_requests_limits(metric_name, values, multiplier):
        """
        Compute metric's requests and limits
        :param metric_name: 'cpu' or 'memory'
        :param values: timeseries workload data
        :param multiplier: multiplier of resource upper bound
        :return: recommended requests, limits
        """
        if metric_name == "memory":
            requests = np.max(values, axis=0) * multiplier
            limits = np.max(values, axis=0) * multiplier
        else:
            requests = np.mean(values, axis=0) * multiplier
            limits = np.max(values, axis=0) * multiplier

        return requests, limits

    @staticmethod
    def __convert_memory_unit(bytes_used):
        """
        Convert memory capacity from bytes to other units like MB.
        :param bytes_used: (int) memory capacity in bytes
        :return: (str) memory capacity with unit type
        """
        unit_map = {
            3: "K", 6: "M", 9: "G",
            12: "T", 15: "P", 18: "E"
        }

        capacity = int(bytes_used)
        unit_name = ""
        root = math.log10(bytes_used)
        for unit in sorted(unit_map, reverse=True):
            if root >= unit:
                capacity = round(bytes_used / (10 ** unit), 2)
                unit_name = unit_map[unit]
                break

        return str(capacity) + unit_name

    @staticmethod
    def __convert_cpu_unit(utilisation):
        """
        Convert cpu utilisation to cpu milli cores.
        :param utilisation: (float) cpu utilisation (0~1)
        :return: (str) cpu milli cores
        """
        return str(int(utilisation * 1000)) + "m"
