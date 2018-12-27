"""
Concrete implementation of stable-policy scheduler

Current Algorithm:

    For a list of pods to be scheduled:

    Step 1: Query:
            a: Info about these pods
            b: pods/nodes' workload prediction

    Step 2: Subtract all pods' workload from their current nodes' workload

    Step 3: Schedule each pod sequentially to the new node according to "stable" policy
            such that the variance of all node workloads after allocating the pod to the
            new node should be minimum among all possible choices of node allocation.

"""

import operator
import copy

from framework.log.logger import Logger
from services.orchestra.recommendation.data_processor import DataProcessor

import numpy as np


class StableScheduler(object):

    """
    Pod scheduler based on workload prediction, adopting stable policy (try
    to minimize workload variance across all nodes)

    """

    def __init__(self, logger=None, processor=None, config=None):

        """ Initialize StableScheduler class """

        self.logger = logger or Logger()
        self.processor = processor or DataProcessor(logger=logger)

        self.config = config
        if self.config is None:
            app_path = os.path.dirname(os.path.abspath(__file__))
            with open(os.path.join(app_path, 'config/recommendation_conf.yaml')) as file_:
                self.config = yaml.load(file_)

        # Initialize metric_names to None. Will be concrete when prediction data is queried.
        self.metric_names = None

    def schedule(self, pod_info):

        """
        Main function for StableScheduler.

        Args:
            pod_info: necessary info about pods that will be scheduled
        Returns:
            allocation_result: scheduling results for the pods

        """

        # query pod/node predicted workload
        pod_predicted_data, node_predicted_data, pods_current_node = \
            self.query_workload_data(pod_info)

        # extract metric names
        self.metric_names = self.get_metric_names(node_predicted_data)

        # subtract all the workload of the rescheduled pods from their current node's workload
        node_workload = self.all_pod_workload_subtraction(pod_predicted_data,
                                                          node_predicted_data,
                                                          pods_current_node)

        # allocate each pod sequentially to new node.
        allocation_result = self.multiple_allocation(pod_predicted_data, node_workload, pod_info)

        return allocation_result

    @staticmethod
    def get_metric_names(node_predicted_data):

        """
        Extract metric_names from node prediction data

        """

        metric_names = list({metric for node_val in node_predicted_data.values()
                             for metric in node_val})

        return metric_names

    def query_workload_data(self, pod_info):

        """
        Query predicted workload data for (a) rescheduled pods (b) cluster nodes.

        Args:
            pod_info: necessary info about pods that will be scheduled
        Returns:
            pod_predicted_data: prediction workload for pods that are to be scheduled
            node_predicted_data: prediction workload for all nodes in the cluster
            pods_current_node: list of node that the pods currently locate in

        """

        pod_predicted_data = []
        pods_current_node = []

        for pod in pod_info:

            pod_predicted_data.append(
                self.processor.query_pod_predicted_data(
                    pod['namespace'], pod['pod_name']))
            pods_current_nodes.append(pod['current_node'])

        node_predicted_data = self.processor.query_nodes_predicted_data()

        return pod_predicted_data, node_predicted_data, pods_current_node

    @staticmethod
    def all_pod_workload_subtraction(pod_predicted_data,
                                     node_predicted_data, pods_current_node):

        """
        Subtract all rescheduled pods' workload from their current nodes' workload.

        Args:
            pod_predicted_data: prediction workload for pods that are to be scheduled
            node_predicted_data: prediction workload for all nodes in the cluster
            pods_current_node: list of node that the pods currently locate in
        Returns:
            node_workload: prediction workload for nodes such that:
                           - subtract all pods' workload from their current nodes
        """

        node_workload = copy.deepcopy(node_predicted_data)

        for i, node_name in enumerate(pods_current_node):
            for metric_name in node_workload[node_name].keys():
                for timestamp in node_workload[node_name][metric_name].keys():
                    node_workload[node_name][metric_name][timestamp] -= \
                        pod_predicted_data[i][metric_name][timestamp]

        return node_workload

    def multiple_allocation(self, pod_predicted_data, node_workload, pod_info):

        """
        Schedule multiple pods sequentially

        Args:
            pod_predicted_data: prediction workload for pods that are to be scheduled
            node_workload: prediction workload for nodes such that:
                           - subtract all pods' workload from their current nodes
            pod_info: necessary info about pods that will be scheduled
        Returns:
            allocation_result: scheduling results for the pods
        """

        allocation_result = {}

        modified_workload = copy.deepcopy(node_workload)

        for i, ind_pod_data in enumerate(pod_predicted_data):

            new_node_name = self.individual_allocation(ind_pod_data, modified_workload)
            allocation_result.update({
                (pod_info[i]['namespace'], pod_info[i]['uid'], pod_info[i]['pod_name']):
                    {"uid": pod_info[i]['uid'],
                     "namespace": pod_info[i]['namespace'],
                     "pod_name": pod_info[i]['pod_name'],
                     "nodes": [new_node_name]}})
            modified_workload = self.pod_workload_addition(ind_pod_data,
                                                           modified_workload,
                                                           new_node_name)

        return allocation_result

    def individual_allocation(self, ind_pod_predicted_data, node_workload):

        """
        Schedule individual pod to appropriate node according to defined
        scoring function.

        Args:
            ind_pod_predicted_data: prediction workload for individual pod
                                    that are to be scheduled
            node_workload: prediction workload for nodes such that:
                           - subtract all pods' workload from their current nodes
                           - add the pod workloads of already-scheduled pods to
                             their newly scheduled nodes
        Returns:
            node_priority_name: The name of the newly scheduled node that the pod
                                will go to
        """

        scores = self.get_node_scores(ind_pod_predicted_data, node_workload)

        node_priority_name = self._allocate_pod(scores)

        return node_priority_name

    def get_node_scores(self, ind_pod_predicted_data, node_workload):

        """
        Get the score of allocating pod to each node

        Args:
            ind_pod_predicted_data: prediction workload for individual pod
                                    that are to be scheduled
            node_workload: prediction workload for nodes such that:
                           - subtract all pods' workload from their current nodes
                           - add the pod workloads of already-scheduled pods to
                             their newly scheduled nodes
        Returns:
            node_scores: a dictionary with scheduling scores for each node, with
                         key = node name, value = scheduling score for that node.
        """

        node_scores = {}

        for node_name, node_data in node_workload.items():
            # add pod's workload to currently iterated node:
            modified_node_workload = self.pod_workload_addition(ind_pod_predicted_data,
                                                                node_workload,
                                                                node_name)

            node_score = self._compute_score(modified_node_workload)
            node_scores.update({node_name: node_score})

        return node_scores

    @staticmethod
    def pod_workload_addition(ind_pod_predicted_data, node_workload, node_name):

        """
        Add a pod's workload to a node

        Args:
            ind_pod_predicted_data: prediction workload for individual pod
                                    that are to be scheduled
            node_workload: prediction workload for nodes such that:
                           - subtract all pods' workload from their current nodes
                           - add the pod workloads of already-scheduled pods to
                             their newly scheduled nodes
            node_name: the name of the node that the pod's workload will be added to
        Returns:
            modified_workload: the node workload with the target pod's workload added
        """

        modified_workload = copy.deepcopy(node_workload)

        for metric_name in modified_workload[node_name].keys():
            for timestamp in modified_workload[node_name][metric_name].keys():
                modified_workload[node_name][metric_name][timestamp] += \
                    ind_pod_predicted_data[metric_name][timestamp]

        return modified_workload

    def _compute_score(self, modified_workload):

        """
        Compute the score of each allocation case.

        Args:
            modified_workload: the node workload with the target pod's workload added
        Returns:
            score: the scheduling score for allocating target pod to some node.
        """

        score = 0.0

        for metric_name in self.metric_names:

            metric_workload = np.array([[modified_workload[node_name][metric_name][timestamp]
                                         for timestamp in modified_workload[node_name][metric_name]]
                                        for node_name in modified_workload.keys()])

            score += self.config["scheduler_weighting"][metric_name] *\
                     np.mean(np.var(metric_workload, axis=0))

        return score

    @staticmethod
    def _allocate_pod(node_scores):

        """
        Return the new node that the rescheduled pod should go to.

        Args:
            node_scores: a dictionary with scheduling scores for each node, with
                         key = node name, value = scheduling score for that node.
        Returns:
            result[0]: The name of the newly scheduled node that the pod will go to
        """

        result, _ = zip(*sorted(node_scores.items(), key=operator.itemgetter(1)))

        return result[0]
