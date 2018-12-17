"""scheduler"""
from abc import ABCMeta, abstractmethod
from framework.log.logger import Logger
from services.orchestra.recommendation.data_processor import DataProcessor


class Scheduler(metaclass=ABCMeta):
    """Scheduler"""
    def __init__(self, logger=None, processor=None):
        self.logger = logger or Logger()
        self.processor = processor or DataProcessor(logger=logger)

        self.weighting = 1  # config parameter

    def schedule(self, pod):
        """Main function to allocate pod to nodes"""

        pod_predicted_data = self.processor.query_pod_predicted_data()
        node_predicted_data = self.processor.query_nodes_predicted_data()
        current_node = ''

        scores = self.get_node_scores(
            pod_predicted_data, node_predicted_data, current_node)
        node_priorities = self.__allocate_pod(scores)
        return node_priorities

    def get_node_scores(self, pod_predicted_data, node_predicted_data, current_node_name):
        """Get every node score"""

        node_scores = {}
        for node_name, node_data in node_predicted_data.items():

            node_score = self.__compute_score(pod_predicted_data, node_data)
            node_scores.update({node_name: node_score})

        return node_scores

    @abstractmethod
    def __compute_score(self, pod_data, node_data):
        """Compute score for priority of one node"""
        return NotImplemented

    @abstractmethod
    def __allocate_pod(self, scores):
        """According to node scores, allocate pod to the appropriate node"""
        return NotImplemented
