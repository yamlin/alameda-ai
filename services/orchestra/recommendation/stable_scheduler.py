""" concrete implementation of stable-policy scheduler  """

import operator
import copy

from framework.log.logger import Logger
from services.orchestra.recommendation.data_processor import DataProcessor

import numpy as np


class StableScheduler(object):

    """ Class for stable-policy scheduler. """

    def __init__(self, logger=None, processor=None, config=None):

        """ Initialize class StableScheduler """

        self.logger = logger or Logger()
        self.processor = processor or DataProcessor(logger=logger)

        self.config = config
        if self.config is None:
            app_path = os.path.dirname(os.path.abspath(__file__))
            with open(os.path.join(app_path, 'config/recommendation_conf.yaml')) as file_:
                self.config = yaml.load(file_)

    def schedule(self, pod_info):

        """ Main function for StableScheduler. """

        # query pod/node predicted workload
        pod_predicted_data, node_predicted_data, pods_current_node = \
            self.query_workload_data(pod_info)

        # substract all the workload of the rescheduled pods from their current node's workload
        node_workload = self.all_pod_workload_substraction(pod_predicted_data,
                                                           node_predicted_data,
                                                           pods_current_node)

        # allocate each pod sequentially to new node.
        allocation_result = self.multiple_allocation(pod_predicted_data, node_workload, pod_info)

        return allocation_result

    def query_workload_data(self, pod_info):

        """ Query predicted workload data for (a) rescheduled pods (b) cluster nodes. """

        pod_predicted_data = []
        pods_current_nodes = []

        for pod in range(len(pod_info)):

            pod_predicted_data.append(
                self.processor.query_pod_predicted_data(
                    pod['namespace'], pod['pod_name']))
            pods_current_nodes.append(pod['current_node'])

        node_predicted_data = self.processor.query_nodes_predicted_data()

        return pod_predicted_data, node_predicted_data, pods_current_nodes

    @staticmethod
    def all_pod_workload_substraction(pod_predicted_data,
                                      node_predicted_data, pods_current_nodes):

        """ Substract all rescheduled pods' workload from their current nodes' workload. """

        node_workload = copy.deepcopy(node_predicted_data)

        for i, node_name in enumerate(pods_current_nodes):
            for metric_name in node_workload[node_name].keys():
                for timestamp in node_workload[node_name][metric_name].keys():
                    node_workload[node_name][metric_name][timestamp] -= \
                        pod_predicted_data[i][metric_name][timestamp]

        return node_workload

    def multiple_allocation(self, pod_predicted_data, node_workload, pod_info):

        """ Allocate multiple pods sequentially """

        allocation_result = {}

        modified_workload = copy.deepcopy(node_workload)

        for i, pod_data in enumerate(pod_predicted_data):

            new_node_name = self.individual_allocation(pod_data, modified_workload)
            allocation_result.update({
                (pod_info[i]['namespace'], pod_info[i]['uid'], pod_info[i]['pod_name']):
                    {"uid": pod_info[i]['uid'],
                     "namespace": pod_info[i]['namespace'],
                     "pod_name": pod_info[i]['pod_name'],
                     "nodes": [new_node_name]}})
            modified_workload = self.pod_workload_addition(pod_data,
                                                           modified_workload,
                                                           new_node_name)

        return allocation_result

    def individual_allocation(self, pod_predicted_data, node_workload):

        """ Allocate individual pod """

        scores = self.get_node_scores(pod_predicted_data, node_workload)

        node_priority_name = self._allocate_pod(scores)

        return node_priority_name

    def get_node_scores(self, pod_predicted_data, node_workload):

        """ Get the score of allocating pod to each node """

        node_scores = {}

        for node_name, node_data in node_workload.items():
            # add pod's workload to currently iterated node:
            modified_node_workload = self.pod_workload_addition(pod_predicted_data,
                                                                node_workload,
                                                                node_name)

            node_score = self._compute_score(modified_node_workload)
            node_scores.update({node_name: node_score})

        return node_scores

    @staticmethod
    def pod_workload_addition(pod_predicted_data, node_workload, node_name):

        """ Add a pod's workload to a node """

        modified_workload = copy.deepcopy(node_workload)

        for metric_name in modified_workload[node_name].keys():
            for timestamp in modified_workload[node_name][metric_name].keys():
                modified_workload[node_name][metric_name][timestamp] += \
                    pod_predicted_data[metric_name][timestamp]

        return modified_workload

    def _compute_score(self, node_data):

        """ Compute the score of each allocation case.  """

        score = 0.0

        for metric_name in self.config["metric_types"].values():

            metric_workload = np.array([[node_data[node_name][metric_name][timestamp]
                                         for timestamp in node_data[node_name][metric_name]]
                                        for node_name in node_data.keys()])

            score += self.config["weighting"][metric_name] *\
                     np.mean(np.var(metric_workload, axis=0))

        return score

    @staticmethod
    def _allocate_pod(scores):

        """ Return the new node that the rescheduled pod should go to. """

        result, _ = zip(*sorted(scores.items(), key=operator.itemgetter(1)))

        return result[0]
