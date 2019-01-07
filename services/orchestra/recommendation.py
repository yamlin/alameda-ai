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

        try:
            allocation_result, scheduling_scores, scheduled_node_data = \
                scheduler.schedule(recommended_pod_list)

            for pod in recommended_results:
                recommended_results[pod].update(allocation_result[pod])
            overall_results = {
                "pod_recommendations": list(recommended_results.values())
            }

            recommender_log.info("Recommendation result")
            recommender_log.info(overall_results)

            recommender_log.info("Scheduled node workload")
            recommender_log.info(scheduled_node_data)

            processor.dao.write_data("container_recommendation", recommended_results)
            processor.dao.write_data("node_prediction", scheduled_node_data)
        except Exception as err:
            recommender_log.error("Error in writing recommendation result via "
                                  "gRPC client: %s %s", type(err), str(err))

        time.sleep(60)

    log.info("Recommendation is completed.")


if __name__ == '__main__':
    main()
