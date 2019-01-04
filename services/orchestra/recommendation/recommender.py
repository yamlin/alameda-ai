"""Recommender"""

import numpy
from framework.log.logger import Logger
from services.orchestra.recommendation.init_detector import InitDetector
from services.orchestra.recommendation.data_processor import DataProcessor


class Recommender:
    """Recommender"""

    def __init__(self, logger=None, processor=None, init_detector=None):
        self.logger = logger or Logger()
        self.processor = processor or DataProcessor(logger=logger)
        self.init_detector = init_detector or InitDetector(logger=logger)

        self.gamma = -1  # config parameter

    def recommend(self, pod):
        """Main function to give recommendation result"""

        namespace = pod["namespace"]
        pod_name = pod["pod_name"]
        self.logger.info("[Recommender] Resource recommendation for pod "
                         "\"%s\"\n", pod)

        # [1] Retrieve prediction data
        predicted_data = self.processor.query_containers_predicted_data(
            namespace, pod_name)
        if not predicted_data:
            self.logger.info("[Recommender] Pod \"%s\" query results is empty; "
                             "thus not recommended\n", pod)
            return False, None

        # [2] Recommendation
        init_resource = self.init_stage(namespace, pod_name)
        resource = self.predict_stage(predicted_data, init_resource)
        resource_spec = self.processor.get_containers_resources(
            namespace, pod_name)

        # [3] Scheduler and write results
        keep = self._keep_recommendation(resource, resource_spec)
        recommend_results = dict()
        if keep:
            recommend_results = self.processor.get_pod_recommendation_result(
                pod, init_resource, resource)
        return keep, recommend_results

    def init_stage(self, namespace, pod_name):
        """
        Find out init stage and compute init stage resources.
        Init stage is a short period after a pod is created that might have
        unstable workload.
        """

        init_resource = dict()

        # Get init stage resource based on init stage observed workload.
        init_data = self.processor.query_containers_init_observed_data(
            namespace, pod_name)
        if isinstance(init_data, dict):
            for container_name, container_data in init_data.items():
                resource_set = {"requests": dict(), "limits": dict()}
                for metric_type, metric_data in container_data.items():
                    metric_data = numpy.array(
                        [metric_data[k] for k in sorted(metric_data)])
                    init_result = self.init_detector.detect(metric_data)
                    if not init_result:
                        break

                    requests, limits = self._requests_limits_rule(
                        metric_type=metric_type,
                        data_mean=init_result["init_mean"],
                        data_max=init_result["init_max"])
                    resource_set["requests"].update({metric_type: requests})
                    resource_set["limits"].update({metric_type: limits})

                init_resource[container_name] = resource_set

        # Get init stage resource from previous recommendation result.
        init_resource_pr = self.processor.get_containers_init_resource(
            namespace, pod_name)
        if isinstance(init_resource_pr, dict):
            for container_name, container_data in init_resource_pr.items():
                if container_name not in init_resource:
                    init_resource[container_name] = container_data

        self.logger.info("[Recommender] Init-stage resource: %s", init_resource)
        return init_resource

    def predict_stage(self, data, init_resource):
        """
        Give resources setting recommendation for future based on
        predicted data and init stage resources.
        """

        timestamp = -1
        for container_name, container_data in data.items():
            for metric_type, metric_data in container_data.items():
                timestamp = min(min(metric_data), timestamp) \
                    if timestamp != -1 else min(metric_data)

        resources = dict()
        for container_name, container_data in data.items():
            resource_set = {"requests": dict(), "limits": dict()}
            for metric_type, metric_data in container_data.items():
                metric_data = numpy.array(
                    [metric_data[k] for k in sorted(metric_data)])

                requests, limits = self._requests_limits_rule(
                    metric_type=metric_type,
                    data_mean=numpy.mean(metric_data),
                    data_max=numpy.max(metric_data))

                if init_resource and init_resource.get(container_name):
                    init_requests = init_resource[container_name][
                        "requests"][metric_type]
                    init_limits = init_resource[container_name][
                        "limits"][metric_type]

                    requests = max(requests, init_requests)
                    limits = max(limits, init_limits)

                resource_set["time"] = timestamp
                resource_set["requests"].update({metric_type: requests})
                resource_set["limits"].update({metric_type: limits})
            resources[container_name] = [resource_set]

        self.logger.info("[Recommender] Predict-stage resource: %s", resources)
        return resources

    def _keep_recommendation(self, resources, resource_pr):
        """To keep new recommendation or not."""

        for container_name in resources:
            # Keep recommendation result when there is no applying any
            # requests/limits to the container.
            if not resource_pr or not resource_pr.get(container_name):
                return True

            # Keep recommendation result when the difference between
            # recommendation and the previous applied resource is larger
            # than certain threshold 'gamma'.

            # Noted: There would be a list of recommendation results.
            # For now, the recommendation list would only contain one result
            # for stable policy.
            # This method will deal with the situation that the recommendation
            # list has more than one result in the future.
            resource_set = resources[container_name][0]
            for metric_type in resource_set["requests"]:

                requests_pr = resource_pr[container_name][
                    "requests"][metric_type]
                limits_pr = resource_pr[container_name][
                    "limits"][metric_type]

                requests = resource_set["requests"][metric_type]
                limits = resource_set["limits"][metric_type]

                if (abs(requests - requests_pr) / requests) > self.gamma:
                    return True
                if (abs(limits - limits_pr) / limits) > self.gamma:
                    return True

        self.logger.info("[Recommender] The resource recommendation is ignored "
                         "since the recommended requests/limits are similar "
                         "with current ones.")
        return False

    @staticmethod
    def _requests_limits_rule(metric_type, data_mean, data_max):

        if metric_type == 'cpu':
            requests = data_mean
            limits = data_max
        elif metric_type == 'memory':
            requests = data_max
            limits = data_max
        else:
            raise NameError(metric_type, "Metric type is not defined.")

        return requests, limits
