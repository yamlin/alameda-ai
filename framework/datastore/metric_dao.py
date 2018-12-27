# pylint: disable=import-error, no-self-use, unused-argument, invalid-name, no-member
''' The Metric DAO '''
import grpc

from alameda_api.v1alpha1.operator import server_pb2, server_pb2_grpc
from framework.log.logger import Logger
from framework.utils.sys_utils import get_metric_server_address

class MockDAO(object):
    # pylint: disable=line-too-long,too-few-public-methods
    ''' Mock Data '''
    def __get_container_init_data(self):
        return {'status': {'code': 0, 'message': '', 'details': []}, 'podMetrics': [{'namespacedName': {'namespace': 'openshit-monitoring', 'name': 'prometheus-k8s-0'}, 'containerMetrics': [{'name': 'prometheus', 'metricData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '64'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '128'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '152'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '176'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '200'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '224'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '64'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '128'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '152'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '176'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '200'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '224'}]}]}]}, {'namespacedName': {'namespace': 'openshit-monitoring', 'name': 'prometheus-k8s-1'}, 'containerMetrics': [{'name': 'prometheus', 'metricData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '25'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '30'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '35'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '40'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '45'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '25'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '30'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '35'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '40'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '45'}]}]}]}]}

    def __get_container_observed_data(self):
        return {'status': {'code': 0, 'message': '', 'details': []}, 'podMetrics': [{'namespacedName': {'namespace': 'openshit-monitoring', 'name': 'prometheus-k8s-0'}, 'containerMetrics': [{'name': 'prometheus', 'metricData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '64'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '128'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '152'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '176'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '200'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '224'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '64'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '128'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '152'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '176'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '200'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '224'}]}]}]}, {'namespacedName': {'namespace': 'openshit-monitoring', 'name': 'prometheus-k8s-1'}, 'containerMetrics': [{'name': 'prometheus', 'metricData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '25'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '30'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '35'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '40'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '45'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '25'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '30'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '35'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '40'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '45'}]}]}]}]}

    def __get_container_predicted_data(self):
        return {'status': {'code': 0, 'message': '', 'details': []}, 'podPredictions': [{'namespacedName': {'namespace': 'openshift-monitoring', 'name': 'prometheus-k8s-0'}, 'containerPredictions': [{'name': 'prometheus', 'predictedRawData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedLimitData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedRequestData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedInitialLimitResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedInitialRequestResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}]}, {'name': 'another-container', 'predictedRawData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedLimitData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedRequestData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedInitialLimitResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedInitialRequestResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}]}]}, {'namespacedName': {'namespace': 'openshift-monitoring', 'name': 'prometheus-k8s-1'}, 'containerPredictions': [{'name': 'prometheus', 'predictedRawData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedLimitData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedRequestData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedInitialLimitResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedInitialRequestResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}]}, {'name': 'another-container', 'predictedRawData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedLimitData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedRequestData': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedInitialLimitResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'predictedInitialRequestResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}]}]}]}

    def __get_node_predicted_data(self):
        return {'status': {'code': 0, 'message': '', 'details': []}, 'nodePredictions': [{'name': 'node1', 'predictedRawData': [{'metricType': 'NODE_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'NODE_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}]}, {'name': 'node2', 'predictedRawData': [{'metricType': 'NODE_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'NODE_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}]}]}

    def __get_node_observed_data(self):
        return {'status': {'code': 0, 'message': '', 'details': []}, 'nodeMetrics': [{'name': 'node1', 'metricData': [{'metricType': 'NODE_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '25'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '30'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '35'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '40'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '45'}]}, {'metricType': 'NODE_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '64'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '128'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '152'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '176'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '200'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '224'}]}]}, {'name': 'node2', 'metricData': [{'metricType': 'NODE_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '25'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '30'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '35'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '40'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '45'}]}, {'metricType': 'NODE_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '64'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '128'}, {'time': '2018-12-26T07:24:20Z', 'numValue': '152'}, {'time': '2018-12-26T07:24:50Z', 'numValue': '176'}, {'time': '2018-12-26T07:25:20Z', 'numValue': '200'}, {'time': '2018-12-26T07:25:50Z', 'numValue': '224'}]}]}]}

    def __get_pod_list_data(self):
        return {'status': {'code': 0, 'message': '', 'details': []}, 'pods': [{'namespacedName': {'namespace': 'openshit-monitoring', 'name': 'prometheus-k8s-0'}, 'containers': [{'name': 'prometheus', 'limitResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'requestResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'limitResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'initialLimitResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'initialRequestResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}]}, {'name': 'another-container', 'limitResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'requestResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'limitResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'initialLimitResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'initialRequestResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}]}]}, {'namespacedName': {'namespace': 'openshit-monitoring', 'name': 'prometheus-k8s-1'}, 'containers': [{'name': 'prometheus', 'limitResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'requestResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'limitResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'initialLimitResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'initialRequestResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}]}, {'name': 'another-container', 'limitResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'requestResource': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'limitResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'initialLimitResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}], 'initialRequestResourceRecommendation': [{'metricType': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '20'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '50'}]}, {'metricType': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'numValue': '512'}, {'time': '2018-12-26T07:23:50Z', 'numValue': '1024'}]}]}]}]}

    def __get_node_list_data(self):
        return {'status': {'code': 0, 'message': '', 'details': []}, 'nodes': [{'name': 'node1'}, {'name': 'node2'}]}

    def __get_container_init_resource(self):
        return {'status': {'code': 0, 'message': '', 'details': []}, 'pod_predictions': [{'namespaced_name': {'namespace': 'openshift-monitoring', 'name': 'prometheus-k8s-0'}, 'container_predictions': [{'name': 'prometheus', 'predicted_raw_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_limit_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_request_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_limit_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_request_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}]}, {'name': 'another-container', 'predicted_raw_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_limit_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_request_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_limit_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_request_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}]}]}, {'namespaced_name': {'namespace': 'openshift-monitoring', 'name': 'prometheus-k8s-1'}, 'container_predictions': [{'name': 'prometheus', 'predicted_raw_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_limit_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_request_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_limit_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_request_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}]}, {'name': 'another-container', 'predicted_raw_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_limit_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_request_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_limit_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_request_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}]}]}]}

    def __get_container_resource(self):
        return {'status': {'code': 0, 'message': '', 'details': []}, 'pod_predictions': [{'namespaced_name': {'namespace': 'openshift-monitoring', 'name': 'prometheus-k8s-0'}, 'container_predictions': [{'name': 'prometheus', 'predicted_raw_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_limit_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_request_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_limit_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_request_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}]}, {'name': 'another-container', 'predicted_raw_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_limit_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_request_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_limit_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_request_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}]}]}, {'namespaced_name': {'namespace': 'openshift-monitoring', 'name': 'prometheus-k8s-1'}, 'container_predictions': [{'name': 'prometheus', 'predicted_raw_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_limit_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_request_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_limit_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_request_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}]}, {'name': 'another-container', 'predicted_raw_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_limit_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_request_data': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_limit_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}], 'predicted_initial_request_resource': [{'metric_type': 'CONTAINER_CPU_USAGE_SECONDS_PERCENTAGE', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '20'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '50'}]}, {'metric_type': 'CONTAINER_MEMORY_USAGE_BYTES', 'data': [{'time': '2018-12-26T07:23:20Z', 'num_value': '512'}, {'time': '2018-12-26T07:23:50Z', 'num_value': '1024'}]}]}]}]}

    def get_data(self, data_type, args):
        ''' Returns the mocked data by data_type '''
        if data_type == "container_init":
            res = self.__get_container_init_data()
        elif data_type == "container_observed":
            res = self.__get_container_observed_data()
        elif data_type == "container_predicted":
            res = self.__get_container_predicted_data()
        elif data_type == "node_predicted":
            res = self.__get_node_predicted_data()
        elif data_type == "node_observed":
            res = self.__get_node_observed_data()
        elif data_type == "pod_list":
            res = self.__get_pod_list_data()
        elif data_type == "node_list":
            res = self.__get_node_list_data()
        elif data_type == "container_init_resource":
            res = self.__get_container_init_resource()
        elif data_type == "container_resource":
            res = self.__get_container_resource()
        else:
            raise Exception("Invalid data type")
        return res


class MetricDAO(object):
    ''' Metric DAO '''

    def __init__(self, config=None):
        ''' The construct methdo '''
        if not config:
            config = {
                "metric_server": get_metric_server_address()
            }
        self.config = config
        self.logger = Logger()
        self.logger.info("Metric DAO config: %s", str(self.config))

    def __get_client(self):
        ''' Get the grpc client '''
        conn_str = self.config["metric_server"]
        channel = grpc.insecure_channel(conn_str)
        return server_pb2_grpc.OperatorServiceStub(channel)

    def __get_metric_type_value(self, metric_type):
        ''' Get the metric type '''
        if metric_type == "cpu":
            key = "CONTAINER_CPU_USAGE_TOTAL"
        elif metric_type == "cpu_rate":
            key = "CONTAINER_CPU_USAGE_TOTAL_RATE"
        elif metric_type == "memory":
            key = "CONTAINER_MEMORY_USAGE"
        else:
            key = "CONTAINER_CPU_USAGE_TOTAL"

        # default return cpu
        return server_pb2.MetricType.Value(key)

    def __get_op_type_value(self, op_type):
        ''' Get the op type '''
        if op_type == "equal":
            key = "Equal"
        else:
            key = "NotEqual"
        return server_pb2.StrOp.Value(key)

    def __parse_prediction(self, data):
        ''' Parse the prediction data '''
        result = server_pb2.PredictData()
        result.time.FromSeconds(data["time"])
        result.value = data["value"]
        return result

    def __parse_recommendation(self, data):
        ''' Parse recommendation '''
        result = server_pb2.Recommendation()
        result.time.FromSeconds(data["time"])
        result.resource.CopyFrom(self.__parse_resource(data["resources"]))
        return result

    def __parse_time_series(self, data):
        ''' Parse time series data '''
        result = server_pb2.TimeSeriesData()
        predict_data = list(map(self.__parse_prediction, data))
        result.predict_data.extend(predict_data)
        return result

    def __parse_container_prediction_data(self, data):
        ''' Parse the container prediction data '''
        if not data:
            return []
        result = server_pb2.PredictContainer()

        if "container_name" in data:
            result.name = data["container_name"]

        if "raw_predict" in data:
            for k, v in data["raw_predict"].items():
                time_series = self.__parse_time_series(v)
                result.row_predict_data[k].CopyFrom(time_series)

        if "recommendations" in data:
            result.recommendations.extend(list(
                map(self.__parse_recommendation, data["recommendations"])
            ))

        if "init_resource" in data:
            result.initial_resource.CopyFrom(
                self.__parse_resource(data["init_resource"])
            )

        return result

    def __parse_resource(self, data):
        ''' Parse the resource data '''
        resource = server_pb2.Resource()

        for k, v in data["limits"].items():
            resource.limit[k] = v

        for k, v in data["requests"].items():
            resource.request[k] = v

        return resource

    def __parse_sample(self, data):
        ''' Parse the sample data '''
        return {
            "time": data.time.seconds,
            "value": data.value
        }

    def __parse_metrics(self, data):
        ''' Parse the metrics '''
        result = []
        if not data:
            return result

        for d in data:
            r = {"labels": {}}
            for k, v in d.labels.items():
                r["labels"][k] = v

            # add the sample data
            r["data"] = list(
                map(self.__parse_sample, d.samples)
            )
            result.append(r)
        return result

    def write_container_prediction_data(self, prediction):
        ''' Write the prediction result to server. '''
        self.logger.info("Write prediction result: %s", str(prediction))
        req = server_pb2.CreatePredictResultRequest()
        pod = req.predict_pods.add()

        pod.uid = prediction["uid"]
        pod.namespace = prediction["namespace"]
        pod.name = prediction["pod_name"]
        pod.predict_containers.extend(list(
            map(self.__parse_container_prediction_data,
                prediction["containers"]
               )
        ))
        try:
            client = self.__get_client()
            resp = client.CreatePredictResult(req)
            if resp.status.code != 0:
                msg = "Write prediction error [code={}]".format(resp.status.code)
                raise Exception(msg)
        except Exception as e:
            self.logger.error("Could not get metrics: %s", str(e))
            raise e

    def write_container_recommendation_result(self, data):
        ''' Write the container recommendation result '''
        self.write_container_prediction_data(data)

    def get_container_observed_data(self, metric_type, namespace_name, pod_name, duration):
        ''' Get the observed metrics '''
        self.logger.info("Get observed data: metric_type=%s, "
                         " namespace=%s, pod_name=%s, duration=%s",
                         str(metric_type), str(namespace_name),
                         str(pod_name), str(duration))
        req = server_pb2.ListMetricsRequest()
        req.metric_type = self.__get_metric_type_value(metric_type)
        req.duration.seconds = duration
        # setup the query conditions
        namespace = req.conditions.add()
        namespace.key = u"namespace"
        namespace.op = self.__get_op_type_value("equal")
        namespace.value = namespace_name
        pod = req.conditions.add()
        pod.key = u"pod_name"
        pod.op = self.__get_op_type_value("equal")
        pod.value = pod_name

        try:
            client = self.__get_client()
            resp = client.ListMetrics(req)
            if resp.status.code == 0:
                return self.__parse_metrics(resp.metrics)
            else:
                msg = "List metric error [code={}]".format(resp.status.code)
                raise Exception(msg)
        except Exception as e:
            self.logger.error("Could not get metrics: %s", str(e))
            raise e
     