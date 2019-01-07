'''Main entrypoint for workload prediction.'''

# pylint: disable=no-name-in-module, broad-except
import time
import traceback
from multiprocessing import Process
from framework.log.logger import Logger, LogLevel
from services.orchestra.recommendation.data_processor import DataProcessor
from services.orchestra.workload_prediction.workload_predictor \
    import WorkloadPredictor


def pod_prediction():
    """Subprocess for pod workload prediction"""

    logger = Logger(name='pod_prediction',
                    logfile='/var/log/workload_prediction_pod.log',
                    level=LogLevel.LV_DEBUG)
    processor = DataProcessor(logger=logger)
    predictor = WorkloadPredictor(log=logger, processor=processor)

    while True:

        pod_list = processor.get_pod_list()
        for pod in pod_list:
            try:
                predictor.predict(pod, 'POD')
            except Exception:
                logger.error(traceback.format_exc())

        time.sleep(30)


def node_prediction():
    """Subprocess for node workload prediction"""

    logger = Logger(name='node_prediction',
                    logfile='/var/log/workload_prediction_node.log',
                    level=LogLevel.LV_DEBUG)
    processor = DataProcessor(logger=logger)
    predictor = WorkloadPredictor(log=logger, processor=processor)

    while True:

        node_list = processor.get_node_list()
        for node in node_list:
            try:
                predictor.predict(node, 'NODE')
            except Exception:
                logger.error(traceback.format_exc())

        time.sleep(30)


def main():
    '''Main entrypoint for workload prediction.'''

    log = Logger()
    log.info("Start workload prediction.")

    p_1 = Process(target=pod_prediction)
    p_2 = Process(target=node_prediction)

    p_1.start()
    p_2.start()
    p_1.join()
    p_2.join()

    log.info("Workload prediction is completed.")


if __name__ == '__main__':
    main()
