# pylint: disable=import-error, no-self-use, unused-argument, invalid-name, no-member
''' The Metric DAO '''
import grpc

from alameda_api.v1alpha1.operator import server_pb2, server_pb2_grpc
from framework.log.logger import Logger
from framework.utils.sys_utils import get_metric_server_address

class MockDAO(object):
    # pylint: disable=line-too-long,too-few-public-methods
    ''' Mock Data '''

    def __init__(self):
        self.logger = Logger()

    def __get_container_init_data(self):
        return {"status": {"code": 0, "message": "", "details": []}, "pod_metrics": [{"namespaced_name": {"namespace": "alameda", "name": "nginx-deployment-f99bb8986-h8rbk"}, "container_metrics": [{"name": "prometheus", "metric_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "64"}, {"time": "2018-12-26T07:23:50Z", "num_value": "128"}, {"time": "2018-12-26T07:24:20Z", "num_value": "152"}, {"time": "2018-12-26T07:24:50Z", "num_value": "176"}, {"time": "2018-12-26T07:25:20Z", "num_value": "200"}, {"time": "2018-12-26T07:25:50Z", "num_value": "224"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "64"}, {"time": "2018-12-26T07:23:50Z", "num_value": "128"}, {"time": "2018-12-26T07:24:20Z", "num_value": "152"}, {"time": "2018-12-26T07:24:50Z", "num_value": "176"}, {"time": "2018-12-26T07:25:20Z", "num_value": "200"}, {"time": "2018-12-26T07:25:50Z", "num_value": "224"}]}]}]}, {"namespaced_name": {"namespace": "alameda", "name": "nginx-deployment-f99bb8986-npg2f"}, "container_metrics": [{"name": "prometheus", "metric_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "25"}, {"time": "2018-12-26T07:24:20Z", "num_value": "30"}, {"time": "2018-12-26T07:24:50Z", "num_value": "35"}, {"time": "2018-12-26T07:25:20Z", "num_value": "40"}, {"time": "2018-12-26T07:25:50Z", "num_value": "45"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "25"}, {"time": "2018-12-26T07:24:20Z", "num_value": "30"}, {"time": "2018-12-26T07:24:50Z", "num_value": "35"}, {"time": "2018-12-26T07:25:20Z", "num_value": "40"}, {"time": "2018-12-26T07:25:50Z", "num_value": "45"}]}]}]}]}

    def __get_container_observed_data(self):
        return {"status": {"code": 0, "message": "", "details": []}, "pod_metrics": [{"namespaced_name": {"namespace": "alameda", "name": "nginx-deployment-f99bb8986-h8rbk"}, "container_metrics": [{"name": "prometheus", "metric_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "64"}, {"time": "2018-12-26T07:23:50Z", "num_value": "128"}, {"time": "2018-12-26T07:24:20Z", "num_value": "152"}, {"time": "2018-12-26T07:24:50Z", "num_value": "176"}, {"time": "2018-12-26T07:25:20Z", "num_value": "200"}, {"time": "2018-12-26T07:25:50Z", "num_value": "224"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "64"}, {"time": "2018-12-26T07:23:50Z", "num_value": "128"}, {"time": "2018-12-26T07:24:20Z", "num_value": "152"}, {"time": "2018-12-26T07:24:50Z", "num_value": "176"}, {"time": "2018-12-26T07:25:20Z", "num_value": "200"}, {"time": "2018-12-26T07:25:50Z", "num_value": "224"}]}]}]}, {"namespaced_name": {"namespace": "alameda", "name": "nginx-deployment-f99bb8986-npg2f"}, "container_metrics": [{"name": "prometheus", "metric_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "25"}, {"time": "2018-12-26T07:24:20Z", "num_value": "30"}, {"time": "2018-12-26T07:24:50Z", "num_value": "35"}, {"time": "2018-12-26T07:25:20Z", "num_value": "40"}, {"time": "2018-12-26T07:25:50Z", "num_value": "45"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "25"}, {"time": "2018-12-26T07:24:20Z", "num_value": "30"}, {"time": "2018-12-26T07:24:50Z", "num_value": "35"}, {"time": "2018-12-26T07:25:20Z", "num_value": "40"}, {"time": "2018-12-26T07:25:50Z", "num_value": "45"}]}]}]}]}

    def __get_container_predicted_data(self):
        return {"status": {"code": 0, "message": "", "details": []}, "pod_predictions": [{"namespaced_name": {"namespace": "alameda", "name": "nginx-deployment-f99bb8986-h8rbk"}, "container_predictions": [{"name": "prometheus", "predicted_raw_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}, {"name": "another-container", "predicted_raw_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}]}, {"namespaced_name": {"namespace": "alameda", "name": "nginx-deployment-f99bb8986-npg2f"}, "container_predictions": [{"name": "prometheus", "predicted_raw_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}, {"name": "another-container", "predicted_raw_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}]}]}

    def __get_node_predicted_data(self):
        return {"status": {"code": 0, "message": "", "details": []}, "node_predictions": [{"name": "node1", "predicted_raw_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "is_scheduled": False}, {"name": "node2", "predicted_raw_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "is_scheduled": False}]}

    def __get_node_observed_data(self):
        return {"status": {"code": 0, "message": "", "details": []}, "node_metrics": [{"name": "node1", "metric_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "25"}, {"time": "2018-12-26T07:24:20Z", "num_value": "30"}, {"time": "2018-12-26T07:24:50Z", "num_value": "35"}, {"time": "2018-12-26T07:25:20Z", "num_value": "40"}, {"time": "2018-12-26T07:25:50Z", "num_value": "45"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "64"}, {"time": "2018-12-26T07:23:50Z", "num_value": "128"}, {"time": "2018-12-26T07:24:20Z", "num_value": "152"}, {"time": "2018-12-26T07:24:50Z", "num_value": "176"}, {"time": "2018-12-26T07:25:20Z", "num_value": "200"}, {"time": "2018-12-26T07:25:50Z", "num_value": "224"}]}]}, {"name": "node2", "metric_data": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "25"}, {"time": "2018-12-26T07:24:20Z", "num_value": "30"}, {"time": "2018-12-26T07:24:50Z", "num_value": "35"}, {"time": "2018-12-26T07:25:20Z", "num_value": "40"}, {"time": "2018-12-26T07:25:50Z", "num_value": "45"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "64"}, {"time": "2018-12-26T07:23:50Z", "num_value": "128"}, {"time": "2018-12-26T07:24:20Z", "num_value": "152"}, {"time": "2018-12-26T07:24:50Z", "num_value": "176"}, {"time": "2018-12-26T07:25:20Z", "num_value": "200"}, {"time": "2018-12-26T07:25:50Z", "num_value": "224"}]}]}]}

    def __get_pod_list_data(self):
        return {"status": {"code": 0, "message": "", "details": []}, "pods": [{"namespaced_name": {"namespace": "alameda", "name": "nginx-deployment-f99bb8986-h8rbk"}, "resource_link": "/namespaces/alameda/deployments/nginx-deployment/replicasets/nginx-deployment-f99bb8986/pods/nginx-deployment-f99bb8986-h8rbk", "containers": [{"name": "prometheus", "limit_resource": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "request_resource": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}, {"name": "another-container", "limit_resource": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "request_resource": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}], "is_alameda": True, "alameda_resource": {"namespace": "alameda", "name": "test-alamedaResource"}, "node_name": "node1", "start_time": "2018-12-26T07:23:20Z", "policy": "STABLE"}, {"namespaced_name": {"namespace": "alameda", "name": "nginx-deployment-f99bb8986-npg2f"}, "resource_link": "/namespaces/alameda/deployments/nginx-deployment/replicasets/nginx-deployment-f99bb8986/pods/nginx-deployment-f99bb8986-npg2f", "containers": [{"name": "prometheus", "limit_resource": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "request_resource": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}, {"name": "another-container", "limit_resource": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "request_resource": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}], "is_alameda": True, "alameda_resource": {"namespace": "alameda", "name": "test-alamedaResource"}, "node_name": "node2", "start_time": "2018-12-26T07:23:20Z", "policy": "STABLE"}]}

    def __get_node_list_data(self):
        return {"status": {"code": 0, "message": "", "details": []}, "nodes": [{"name": "node1"}, {"name": "node2"}]}

    def __get_container_resource(self):
        return {"status": {"code": 0, "message": "", "details": []}, "pod_recommendations": [{"namespaced_name": {"namespace": "alameda", "name": "nginx-deployment-f99bb8986-h8rbk"}, "assign_pod_policy": {"time": "2018-12-26T07:23:20Z", "node_name": "node1"}, "container_recommendations": [{"name": "prometheus", "limit_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "request_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "initial_limit_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "initial_request_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}, {"name": "another-container", "limit_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "request_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "initial_limit_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "initial_request_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}], "apply_recommendation_now": False}, {"namespaced_name": {"namespace": "alameda", "name": "nginx-deployment-f99bb8986-npg2f"}, "assign_pod_policy": {"time": "2018-12-26T07:23:20Z", "node_name": "node2"}, "container_recommendations": [{"name": "prometheus", "limit_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "request_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "initial_limit_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "initial_request_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}, {"name": "another-container", "limit_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "request_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "initial_limit_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}], "initial_request_recommendations": [{"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "20"}, {"time": "2018-12-26T07:23:50Z", "num_value": "50"}]}, {"metric_type": "MEMORY_USAGE_BYTES", "data": [{"time": "2018-12-26T07:23:20Z", "num_value": "512"}, {"time": "2018-12-26T07:23:50Z", "num_value": "1024"}]}]}], "apply_recommendation_now": False}]}

    def get_data(self, data_type, args):
        ''' Returns the mocked data by data_type '''

        self.logger.info("[DAO] get_data: 'data_type'=%s, 'args'=%s", data_type, args)

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
        elif data_type == "container_recommendation":
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
     