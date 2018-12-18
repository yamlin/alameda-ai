'''Main entrypoint for workload prediction.'''

# pylint: disable=E0611
import time
from framework.log.logger import Logger, LogLevel
from framework.datastore.file_dao import FileDataStore
from services.orchestra.workload_prediction.workload_predictor \
    import WorkloadPredictor


def main():
    '''Main entrypoint for workload prediction.'''

    log = Logger()
    log.info("Start workload prediction.")

    # workload predictor
    predictor_log = Logger(name='workload_prediction',
                           logfile='/var/log/workload_prediction.log',
                           level=LogLevel.LV_DEBUG)
    predictor = WorkloadPredictor(log=predictor_log)

    # file datastore to get pod list
    dao = FileDataStore()

    while True:

        pod_list = dao.read_data()
        for k, v in pod_list.items():
            try:
                pod = {
                    "namespace": k[0],
                    "uid": k[1],
                    "pod_name": k[2],
                    "type": v["type"],
                    "policy": v["policy"]
                }
            except (IndexError, KeyError):
                log.error("Not predicting POD %s:%s, "
                          "due to wrong format of pod info.", k, v)
                continue

            predictor.predict(pod)

        time.sleep(60)

    log.info("Workload prediction is completed.")


if __name__ == '__main__':
    main()
