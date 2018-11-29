# -*- coding: utf-8 -*-
""" Recommendation """
# pylint: disable=E0401
import math
import numpy as np
from services.arima.workload_prediction.workload_utils import \
    get_csv_data, get_container_name, get_metric_name_and_conf

CPU = "cpu"
MEM = "memory"


def get_container_recommendation_result(log, pod_info,
                                        input_file_list, output_file_list,
                                        measurement_conf, filename_tags_map,
                                        time_scaling_sec):
    """Get containers recommendation result from specific pod."""

    # multipliers to set the resources
    tolerance = 1.2
    prdt_tolerance = 1.2

    out_data = {
        "uid": pod_info.get("uid"),
        "namespace": pod_info.get("namespace"),
        "pod_name": pod_info.get("pod_name"),
        "containers": []
    }

    container_set = dict()
    for file_path in input_file_list:
        # [1] Read observed data by identical container and metric.
        data = get_csv_data(file_path)
        num_sample, _ = data.shape
        time = int(data[-1, 0] * time_scaling_sec)
        values = data[:, 1]

        # [1.1] Get corresponding container name and metric name.
        container_name = get_container_name(file_path, filename_tags_map)
        metric_name, _ = get_metric_name_and_conf(
            file_path, measurement_conf)

        if container_name is None or metric_name is None:
            continue
        if metric_name not in (CPU, MEM):
            log.warning("[Resource Recommendor] Metric \"%s\" "
                        "are not supported", metric_name)
            continue

        # [2] When this container is running more than specific time
        # (5*60 seconds for now), recommendor would start to compute
        # recommended resources.
        # 5*60 should be parameter
        if num_sample >= ((5*60) // time_scaling_sec):

            # [2.1] Compute recommended resources.
            requests, limits = \
                compute_requests_limits(metric_name, values, tolerance)

            # [2.2] When there's prediction result for this container, then
            # recommendor would consider the predicted workload, too.
            prdt_file_path = '{}.prdt'.format(
                file_path.replace("input", "output"))
            if prdt_file_path in output_file_list:
                data = get_csv_data(file_path)
                values = data[:, 1]

                prdt_requests, prdt_limits = compute_requests_limits(
                    metric_name, values, prdt_tolerance)

                # Maximum between observed data and predicted data.
                requests = max(requests, prdt_requests)
                limits = max(limits, prdt_limits)

            # [3] Format the recommended resources result by container.
            resource_set = format_container_resources(
                metric_name, time, requests, limits)
            if not resource_set:
                log.warning("[Resource Recommendor] Metric \"%s\" "
                            "are not supported", metric_name)
                continue

            if container_name not in container_set:
                container_set[container_name] = resource_set
            else:
                container_set[container_name].update(resource_set)

    # [4] Format the pod recommendation result again.
    container_result = format_container_recommendation_result(container_set)
    if container_result:
        out_data["containers"] = list(container_result.values())
        log.debug("Write data: %s", out_data)
        return out_data

    return None


def compute_requests_limits(metric_name, values, multiplier):
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


def convert_memory_unit(bytes_used):
    """
    Convert memory capacity from bytes to other units like MB.
    :param bytes_used: (int) memory capacity in bytes
    :return: (str) memory capacity with unit type
    """
    unit = {
        3: "K", 6: "M", 9: "G",
        12: "T", 15: "P", 18: "E"
    }

    capacity = int(bytes_used)
    unit_name = ""
    root = math.log10(bytes_used)
    for u in sorted(unit, reverse=True):
        if root >= u:
            capacity = round(bytes_used / (10 ** u), 2)
            unit_name = unit[u]
            break

    return str(capacity) + unit_name


def convert_cpu_unit(utilisation):
    """
    Convert cpu utilisation to cpu milli cores.
    :param utilisation: (float) cpu utilisation (0~1)
    :return: (str) cpu milli cores
    """
    return str(int(utilisation * 1000)) + "m"


def format_container_resources(metric_name, time, requests, limits):
    """Format container recommended resources to meet the spec of k8s"""
    resource_set = {}
    if metric_name == MEM:
        resource_set = {
            metric_name: {
                "time": time,
                "requests": convert_memory_unit(requests),
                "limits": convert_memory_unit(limits)
            }
        }
    elif metric_name == CPU:
        resource_set = {
            metric_name: {
                "time": time,
                "requests": convert_cpu_unit(requests),
                "limits": convert_cpu_unit(limits)
            }
        }

    return resource_set


def format_container_recommendation_result(container_set):
    """Format the recommendation result to write data by GRPC client."""
    container_recommendation_set = dict()
    for container_name in container_set:
        recommendation_set = {
            "time": 0,
            "resources": {
                "limits": dict(),
                "requests": dict()
            }
        }
        init_set = {
            "limits": dict(),
            "requests": dict()
        }

        time = 0
        for metric_name, metric_set in container_set[container_name].items():
            time = max(metric_set["time"], time)

            recommendation_set["resources"]["limits"].update(
                {metric_name: metric_set["limits"]})
            recommendation_set["resources"]["requests"].update(
                {metric_name: metric_set["requests"]})

            init_set["limits"].update(
                {metric_name: metric_set["limits"]})
            init_set["requests"].update(
                {metric_name: metric_set["requests"]})

        recommendation_set["time"] = time
        container_recommendation_set[container_name] = {
            "container_name": container_name,
            "recommendations": [recommendation_set],
            "init_resource": init_set
        }

    return container_recommendation_set
