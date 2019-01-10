'''Main entrypoint for recommendation.'''

# pylint: disable=no-name-in-module, broad-except
import time
import traceback
from framework.log.logger import Logger
from services.orchestra.recommendation.data_processor import DataProcessor
from services.orchestra.recommendation.recommender import Recommender
from services.orchestra.recommendation.stable_scheduler import StableScheduler


def main():
    '''Main entrypoint for recommendation.'''

    log = Logger()
    log.info("Start recommendation.")

    # recommender log
    recommender_log = Logger(name='recommendation',
                             logfile='/var/log/recommendation.log')
    processor = DataProcessor(logger=recommender_log)
    recommender = Recommender(logger=recommender_log, processor=processor)
    scheduler = StableScheduler(logger=recommender_log, processor=processor)

    while True:
        recommended_pod_list = []
        recommended_results = dict()

        pod_list = processor.get_pod_list()
        for pod in pod_list:
            try:
                success, result = recommender.recommend(pod)
                if success:
                    recommended_pod_list.append(pod)

                    key = processor.get_pod_identifier(pod)
                    recommended_results[key] = result
            except Exception:
                recommender_log.error(traceback.format_exc())

        if recommended_pod_list:
            try:
                success, allocation_result, scores, scheduled_node_data = \
                    scheduler.schedule(recommended_pod_list)

                if success:
                    processor.write_pod_recommendation_result(
                        vpa_result=recommended_results,
                        scheduler_result=allocation_result)

                    recommender_log.info("Scheduled node workload: %s",
                                         scheduled_node_data)
                    processor.dao.write_data("node_prediction",
                                             scheduled_node_data)

                    recommender_log.info("Scheduled score: %s", scores)
                    processor.dao.write_data("simulated_scheduling_score",
                                             scores)
                else:
                    processor.write_pod_recommendation_result(
                        vpa_result=recommended_results)
            except Exception:
                recommender_log.error(traceback.format_exc())

        time.sleep(300)

    log.info("Recommendation is completed.")


if __name__ == '__main__':
    main()
