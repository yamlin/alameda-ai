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
from random import shuffle
import operator
import copy
import os
import yaml

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

        # Initialize scheduling_time to None. Will be concrete when calling "get_scheduling_time"
        self.scheduling_time = None

    def schedule(self, pod_info):

        """
        Main function for StableScheduler.

        """

        allocation_result = None
        scheduling_scores = None
        scheduled_node_data = None

        # query pod/node predicted workload
        success, pod_predicted_data, node_predicted_data, pods_current_node = \
            self.query_workload_data(pod_info)

        # case: any of the necessary data for scheduling is empty.
        if not success:
            return success, allocation_result, scheduling_scores, scheduled_node_data

        else:
            # extract metric names
            self.metric_names = self.get_metric_names(node_predicted_data)
            # get scheduling time
            self.scheduling_time = self.get_scheduling_time(node_predicted_data)

            # subtract all the workload of the rescheduled pods from their current node's workload
            node_workload = self.all_pod_workload_subtraction(pod_predicted_data,
                                                              node_predicted_data,
                                                              pods_current_node)

            allocation_result, scheduling_scores, scheduled_node_data = \
                self.schedule_greedy(pod_predicted_data, node_predicted_data,
                                     node_workload, pod_info)

            return success, allocation_result, scheduling_scores, scheduled_node_data

    def schedule_greedy(self, pod_predicted_data, node_predicted_data, node_workload, pod_info,
                        shuffle_num=50):

        """
        Greedy search to iterate through all possible scheduling orders of pods

        """
        all_allocation_result = []
        all_scheduling_scores = []
        all_scheduled_node_data = []

        for _ in range(shuffle_num):

            pod_info_copy = copy.deepcopy(pod_info)
            pod_data_copy = copy.deepcopy(pod_predicted_data)

            temp = list(zip(pod_info_copy, pod_data_copy))
            shuffle(temp)
            pod_info_copy, pod_data_copy = zip(*temp)

            allocation_result, modified_node_data = self.multiple_allocation(pod_data_copy,
                                                                             node_workload,
                                                                             pod_info_copy)

            # calculate scheduling scores before/after pod schedulings:
            scheduling_scores = self.get_scheduling_scores(node_predicted_data, modified_node_data)

            all_allocation_result.append(allocation_result)
            all_scheduling_scores.append(scheduling_scores)
            all_scheduled_node_data.append(modified_node_data)

        # output the results with minimum score
        best_allocation_result = all_allocation_result[0]
        best_scheduling_scores = all_scheduling_scores[0]
        best_scheduled_node_data = all_scheduled_node_data[0]

        for i in range(shuffle_num):
            if all_scheduling_scores[i]['score_after'] < \
                    best_scheduling_scores['score_after']:
                best_scheduling_scores = all_scheduling_scores[i]
                best_allocation_result = all_allocation_result[i]
                best_scheduled_node_data = all_scheduled_node_data[i]

        best_scheduled_node_data = self.node_workload_formatter(best_scheduled_node_data)

        return best_allocation_result, best_scheduling_scores, best_scheduled_node_data


    @staticmethod
    def get_metric_names(node_predicted_data):

        """
        Extract metric_names from node prediction data

        Args:
            node_predicted_data: prediction workload for all nodes in the cluster
        Returns:
            metric_names: a list of metric names in the queried workload.

        """

        metric_names = list({metric for node_val in node_predicted_data.values()
                             for metric in node_val})

        return metric_names

    def query_workload_data(self, pod_info):

        """
        Query predicted workload data for (a) rescheduled pods (b) cluster nodes.

        Returns:
            pod_predicted_data: prediction workload for pods that are to be scheduled
            node_predicted_data: prediction workload for all nodes in the cluster
            pods_current_node: list of node that the pods currently locate in
            pod_info: detailed information about pods that are to be scheduled.

        """
        # indicator for whether the querying process is successful
        success = True

        pod_predicted_data = []
        pods_current_node = []

        for pod in pod_info:

            data = self.processor.query_pod_predicted_data(pod)

            if not data:
                self.logger.info('[Scheduler] pod: %s is empty', pod)
                continue

            pod_predicted_data.append(self.processor.query_pod_predicted_data(pod))
            pods_current_node.append(pod['node_name'])

        # get available node list in the cluster
        node_list = self.processor.get_node_list()
        node_predicted_data = self.processor.query_nodes_predicted_data(node_list)

        if not pod_predicted_data:
            self.logger.info('[Scheduler] pod predicted data is empty, thus not scheduled')
            success = False
        if not node_predicted_data:
            self.logger.info('[Scheduler] node predicted data is empty, thus not scheduled')
            success = False

        return success, pod_predicted_data, node_predicted_data, pods_current_node

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
            modified_workload: node workloads after scheduling
        """

        allocation_result = {}

        modified_workload = copy.deepcopy(node_workload)

        for i, ind_pod_data in enumerate(pod_predicted_data):

            new_node_name = self.individual_allocation(ind_pod_data, modified_workload)

            key = self.processor.get_pod_identifier(pod_info[i])
            val = {"namespaced_name": pod_info[i]['namespaced_name'],
                   "apply_recommendation_now": True,
                   "assign_pod_policy": {"time": self.scheduling_time,
                                         "node_name": new_node_name}
                  }

            allocation_result.update({key:val})

            modified_workload = self.pod_workload_addition(ind_pod_data,
                                                           modified_workload,
                                                           new_node_name)

        return allocation_result, modified_workload

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

    def get_scheduling_time(self, node_predicted_data):

        """
        Get the scheduling time

        Args:
            node_predicted_data: prediction workload for all nodes in the cluster
        Returns:
            scheduling_time: timestamp for the scheduling this time.

        """

        # get earliest time among the timestamp of node_predicted_data
        timestamps = [i for i in node_predicted_data.values()][0][self.metric_names[0]].keys()
        scheduling_time = self.processor.convert_time(min(timestamps) *
                                                      self.config['data_granularity_sec'])

        return scheduling_time

    def get_scheduling_scores(self, node_predicted_data, modified_node_data):

        """
        Get the scoring comparison before/after scheduling

        Args:
            node_predicted_data: prediction workload for all nodes
                                 in the cluster (before scheduling)
            modified_node_data: prediction workload for all nodes
                                 in the cluster (after scheduling)

        Returns:
            scheduling_scores: a dict specifying the comparison of scores before/after scheduling.
                               In stable policy, the score = variance of node workload predictions
                                                             across nodes.

        """

        score_before = self._compute_score(node_predicted_data)
        score_after = self._compute_score(modified_node_data)

        scheduling_scores = {'score_before': score_before,
                             'score_after': score_after,
                             'time': self.scheduling_time}

        return scheduling_scores

    def node_workload_formatter(self, modified_node_data):

        """
        Format the scheduled node workload into suitable format for returning to operators.

        Args:
            modified_node_data: prediction workload for all nodes in the cluster (after scheduling)

        Returns:
            format_node_workload: Formatted form of modified_node_data

        """

        # get available node list in the cluster
        node_list = [i['name'] for i in self.processor.get_node_list()]

        node_val = []

        for node_name in node_list:

            node_info = {}

            node_workload_data = modified_node_data[node_name]

            # update key "name":
            node_info.update({'name': node_name})

            # update key "predicted_raw_data":
            raw_data_val = []

            for metric_name, values in node_workload_data.items():

                workload = {}
                workload.update({'metric_type': metric_name})

                # modify workload values:
                time_val = []

                # sort values by key (the order of time):
                values = sorted(values.items(), key=operator.itemgetter(0))

                for time, val in values:
                    timestamp = self.processor.convert_time(time *
                                                            self.config['data_granularity_sec'])
                    time_val.append({'time': timestamp,
                                     'num_value': str(val)})

                workload.update({'data': time_val})

                raw_data_val.append(workload)

            node_info.update({'predicted_raw_data': raw_data_val})

            # update key "is_scheduled":
            node_info.update({'is_scheduled': True})

            node_val.append(node_info)

        format_node_workload = {'node_predictions': node_val}

        return format_node_workload
