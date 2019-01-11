'''Unit test for datahub client.'''

import logging
import unittest
from datetime import datetime, timezone, timedelta
import grpc
from alameda_api.v1alpha1.datahub import server_pb2, server_pb2_grpc
from framework.datahub.client import DatahubClient


@unittest.skip
class DatahubClientTestCase(unittest.TestCase):
    '''Unit test for datahub client.'''

    GET_METHOD = 'get'
    PUT_METHOD = 'put'

    def setUp(self):
        '''Setup unittest environment.'''

        self.client = DatahubClient(config={
            "server": "localhost:50050"
        })

        local_time = datetime.now(timezone.utc).astimezone()
        start_time = local_time - timedelta(minutes=20)
        end_time = local_time

        self.common_mock_data = {
            "namespaced_name": {
                "namespace": "openshift-monitoring",
                "name":"prometheus-k8s-0"
            },
            "current_time": local_time.isoformat(),
            "time_range": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            },
            "node_name": ["node1", "node2"],
        }

    def tearDown(self):
        '''Clean unittest environment.'''

        self.client = None

    def __impl_test(self, method, key, data):
        '''Implementation of all tests.

        Args:
            method(str): The target method.
            key(str): The target key to test.
            data(dict): Data to be sent.

        Returns:
            error: True if error happens; Otherwise, return False.
            response: The response from the gRPC server.
        '''

        func = None

        if method == self.GET_METHOD:
            func = self.client.get_data
        elif method == self.PUT_METHOD:
            func = self.client.write_data
        else:
            print("[UnitTest] Unknwon target method '%s'" % method)
            return True, None

        try:
            response = func(key, data)

        except grpc._channel._Rendezvous as error:  # pylint: disable=W0212
            if error.code() == grpc.StatusCode.UNAVAILABLE:
                print("[UnitTest] Cannot access gRPC server. Skip!")
                return False, None

            raise error

        if 'status' in response:
            self.assertTrue(response['status']['code'] == 0)
        else:
            self.assertTrue(response['code'] == 0)

        return False, response

    # Test get methods:
    def test_get_container_init(self):
        '''Test on "container_init".'''

        test_key = "container_init"
        test_data = {
            "namespaced_name": self.common_mock_data['namespaced_name'],
            "time_range": self.common_mock_data['time_range']
        }

        err, res = self.__impl_test(self.GET_METHOD, test_key, test_data)
        self.assertFalse(err)

        # Skip some exceptions due to lack of resources
        if not err and res is None:
            return

        # Validate its response format
        self.assertTrue('pod_metrics' in res)

    def test_get_container_observed(self):
        '''Test on "container_observed".'''

        test_key = "container_observed"
        test_data = {
            "namespaced_name": self.common_mock_data['namespaced_name'],
            "time_range": self.common_mock_data['time_range']
        }

        err, res = self.__impl_test(self.GET_METHOD, test_key, test_data)
        self.assertFalse(err)

        # Skip some exceptions due to lack of resources
        if not err and res is None:
            return

        # Validate its response format
        self.assertTrue('pod_metrics' in res)

    def test_get_container_predicted(self):
        '''Test on "container_predicted".'''

        test_key = "container_predicted"
        test_data = {
            "namespaced_name": self.common_mock_data['namespaced_name'],
            "time_range": self.common_mock_data['time_range']
        }

        err, res = self.__impl_test(self.GET_METHOD, test_key, test_data)
        self.assertFalse(err)

        # Skip some exceptions due to lack of resources
        if not err and res is None:
            return

        # Validate its response format
        self.assertTrue('pod_predictions' in res)

        if 'pod_predictions' in res and res['pod_predictions']:
            for item in res['pod_predictions']:
                self.assertTrue('namespace' in item['namespaced_name'])
                self.assertTrue('name' in item['namespaced_name'])

                if not item['container_predictions']:
                    continue

                for inner_item in item['container_predictions']:
                    self.assertTrue('name' in inner_item)
                    self.assertTrue(inner_item['predicted_raw_data'])

    def test_get_node_observed(self):
        '''Test on "node_observed".'''

        test_key = "node_observed"
        test_data = {
            "node_names": self.common_mock_data['node_name'],
            "time_range": self.common_mock_data['time_range']
        }

        err, res = self.__impl_test(self.GET_METHOD, test_key, test_data)
        self.assertFalse(err)

        # Skip some exceptions due to lack of resources
        if not err and res is None:
            return

        # Validate its response format
        self.assertTrue('node_metrics' in res)

    def test_get_node_predicted(self):
        '''Test on "node_predicted".'''

        test_key = "node_predicted"
        test_data = {
            "node_name": self.common_mock_data['node_name'],
            "time_range": self.common_mock_data['time_range']
        }

        err, res = self.__impl_test(self.GET_METHOD, test_key, test_data)
        self.assertFalse(err)

        # Skip some exceptions due to lack of resources
        if not err and res is None:
            return

        # Validate its response format
        self.assertTrue('node_predictions' in res)

    def test_get_pod_list(self):
        '''Test on "pod_list".'''

        test_key = "pod_list"
        test_data = {}

        err, res = self.__impl_test(self.GET_METHOD, test_key, test_data)
        self.assertFalse(err)

        # Skip some exceptions due to lack of resources
        if not err and res is None:
            return

        # Validate its response format
        self.assertTrue('pods' in res)

    def test_get_node_list(self):
        '''Test on "node_list".'''

        test_key = "node_list"
        test_data = {}

        err, res = self.__impl_test(self.GET_METHOD, test_key, test_data)
        self.assertFalse(err)

        # Skip some exceptions due to lack of resources
        if not err and res is None:
            return

        # Validate its response format
        self.assertTrue('nodes' in res)

        if 'nodes' in res and res['nodes']:
            for item in res['nodes']:
                self.assertTrue('name' in item)

    def test_get_container_recommendation(self):
        '''Test on "container_recommendation".'''

        test_key = "container_recommendation"
        test_data = {
            "namespaced_name": self.common_mock_data['namespaced_name'],
            "time_range": self.common_mock_data['time_range']
        }

        err, res = self.__impl_test(self.GET_METHOD, test_key, test_data)
        self.assertFalse(err)

        # Skip some exceptions due to lack of resources
        if not err and res is None:
            return

        # Validate its response format
        self.assertTrue('pod_recommendations' in res)

    # Test put methods:
    def test_put_container_prediction(self):
        '''Test on "container_prediction".'''

        test_key = "container_prediction"
        test_data = {
            "pod_predictions": [
                {
                    "namespaced_name": self.common_mock_data['namespaced_name'],
                    "container_predictions": [
                        {
                            "name": 'prometheus',
                            "predicted_raw_data": [
                                {
                                    "metric_type": 1,
                                    "data": [
                                        {
                                            "time": self.common_mock_data['current_time'],
                                            "num_value": '10'
                                        }
                                    ]
                                }, {
                                    "metric_type": 2,
                                    "data": [
                                        {
                                            "time": self.common_mock_data['current_time'],
                                            "num_value": '11'
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        err, res = self.__impl_test(self.PUT_METHOD, test_key, test_data)
        self.assertFalse(err)

        # Skip some exceptions due to lack of resources
        if not err and res is None:
            return

        # Needs more test on this item. For example, put & get to confirm data
        # is successfully stored.

    def test_put_node_prediction(self):
        '''Test on "node_prediction".'''

        test_key = "node_prediction"
        test_data = {
            "node_predictions": [
                {
                    "name": "unittest",
                    "predicted_raw_data": [{
                        "metric_type": "CPU_USAGE_SECONDS_PERCENTAGE",
                        "data": [{
                            "time": self.common_mock_data['current_time'],
                            "num_value": "1024"
                        }]
                    }]
                }
            ]
        }

        err, res = self.__impl_test(self.PUT_METHOD, test_key, test_data)
        self.assertFalse(err)

        # Skip some exceptions due to lack of resources
        if not err and res is None:
            return

        # Needs more test on this item. For example, put & get to confirm data
        # is successfully stored.

    def test_put_container_recommendation(self):
        '''Test on "container_recommendation".'''

        test_key = "container_recommendation"
        test_data = {
            "pod_recommendations": [{
                "namespaced_name": self.common_mock_data['namespaced_name'],
                "apply_recommendation_now": True,
                "assign_pod_policy": {
                    "time": self.common_mock_data['current_time'],
                    "node_name": "unittest"
                },
                "container_recommendations": [{
                    "name": "test",
                    "limit_recommendations": [
                        {
                            "metric_type": "CPU_USAGE_SECONDS_PERCENTAGE",
                            "data": [{
                                "time": self.common_mock_data['current_time'],
                                "num_value": "1024"
                            }]
                        }
                    ],
                    "request_recommendations": [{
                        "metric_type": "CPU_USAGE_SECONDS_PERCENTAGE",
                        "data": [{
                            "time": self.common_mock_data['current_time'],
                            "num_value": "1024"
                        }]
                    }],
                    "initial_limit_recommendations": [{
                        "metric_type": "CPU_USAGE_SECONDS_PERCENTAGE",
                        "data": [{
                            "time": self.common_mock_data['current_time'],
                            "num_value": "1024"
                        }]
                    }],
                    "initial_request_recommendations": [{
                        "metric_type": "CPU_USAGE_SECONDS_PERCENTAGE",
                        "data": [{
                            "time": self.common_mock_data['current_time'],
                            "num_value": "1024"
                        }]
                    }]
                }]
            }]
        }

        err, res = self.__impl_test(self.PUT_METHOD, test_key, test_data)
        self.assertFalse(err)

        # Skip some exceptions due to lack of resources
        if not err and res is None:
            return

        # Needs more test on this item. For example, put & get to confirm data
        # is successfully stored.


if __name__ == '__main__':
    unittest.main(verbosity=2)
