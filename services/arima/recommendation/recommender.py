"""Recommender"""

from framework.log.logger import Logger
from services.arima.recommendation.scheduler import Scheduler
from services.arima.recommendation.data_processor import DataProcessor


class Recommender:
    """Recommender"""
    def __init__(self, logger=None, processor=None, scheduler=None):
        self.logger = logger or Logger()
        self.processor = processor or DataProcessor(logger=logger)
        self.scheduler = scheduler or Scheduler(
            logger=logger, processor=processor)

        self.gamma = 0.1  # config parameter

    def recommend(self, pod):
        """Main function to give recommendation result"""

        # [1] Retrieve prediction data
        predicted_data = self.processor.query_containers_predicted_data()
        if not predicted_data:
            self.logger.debug('Pod "%s" query results is empty; '
                              'thus not recommended\n', pod)
            return

        # [2] Recommendation
        init_data = self.processor.query_containers_init_observed_data()
        if init_data:
            init_resource = self.init_stage(init_data)
        else:
            init_resource = self.processor.get_container_init_resource()

        resource = self.predict_stage(predicted_data, init_resource)
        resource_spec = self.processor.get_container_resources()

        # [3] Scheduler and write results
        if self.ignore(resource, resource_spec):
            self.scheduler.schedule(pod)
            self.processor.write_pod_recommendation_result()

    def init_stage(self, data):
        """
        Find out init stage and compute init stage resources.
        Init stage is a short period after a pod is created that might have
        unstable workload.
        """

        return self.__compute_requests_limits(data)

    def predict_stage(self, data, init_resource):
        """
        Give resources setting recommendation for future based on
        predicted data and init stage resources.
        """

        requests, limits = self.__compute_requests_limits(data)
        if init_resource is not None:
            init_requests, init_limits = init_resource
            return max(requests, init_requests), max(limits, init_limits)

        return requests, limits

    def ignore(self, resource, resource_spec):
        """To ignore new recommendation or not."""

        requests, limits = resource
        requests_sp, limits_sp = resource_spec
        if (abs(requests - requests_sp) / requests) < self.gamma or \
           (abs(limits - limits_sp) / limits) < self.gamma:
            return True

        return False

    @staticmethod
    def __compute_requests_limits(data):
        """Compute metric's requests and limits"""
        requests = 0
        limits = 0

        return requests, limits
