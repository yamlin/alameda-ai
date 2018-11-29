# -*- coding: utf-8 -*-
""" Workload predictor """
# pylint: disable=E0401
import os
import sys
from datetime import datetime
from multiprocessing import Process
import shutil
import uuid
import yaml
import regex as re


from framework.log.logger import Logger, LogLevel
from framework.datastore.metric_dao import MetricDAO
from services.arima.workload_prediction.process_threading \
    import predict_by_series
from services.arima.workload_prediction.preprocessor import Preprocessor
from services.arima.workload_prediction.workload_utils \
    import get_csv_data, get_container_name, get_metric_name_and_conf
from services.arima.workload_prediction.recommendation \
    import get_mock_recommendation


class WorkloadPredictor:
    """ Workload predictor """

    def __init__(self, log=None, dao=None, preprocesser=None):
        # Max filename length of linux is 255;
        # reserve capacity 20 character for further name appending
        # e.g., .prdt_log in _predict_write_influx()
        self.MAX_FILENAME_LENGTH = 235
        self.APP_PATH = os.path.dirname(os.path.abspath(__file__))

        with open(os.path.join(
                self.APP_PATH, 'config/measurement_conf.yaml')) as yaml_file:
            measurement_conf = yaml.load(yaml_file)
        self.measurement_conf = measurement_conf

        with open(os.path.join(
                self.APP_PATH, 'config/granularity_conf.yaml')) as yaml_file:
            granularity_conf = yaml.load(yaml_file)
            granularity_conf = granularity_conf['30s']
        self.granularity_conf = granularity_conf

        self.log = log or Logger(name='workload_prediction',
                                 logfile='/var/log/workload_prediction.log',
                                 level=LogLevel.LV_DEBUG)
        self.dao = dao or MetricDAO()
        self.preprocessor = preprocesser or Preprocessor()
        self.target_metrics = measurement_conf.keys()

    def predict(self, pod, thread_num=1, target_labels=None):
        """ Prediction
        :param pod: (dict) the info of the pod that need train/predict.
        :param thread_num: the number of threading.
        :param target_labels: (list) target labels.
        """

        current_time = datetime.now().strftime('%Y%m%d%H%M%S')
        file_folder_name = {
            'input': 'bridge/prediction/input/{}/{}/'.format(
                self.granularity_conf['mid'], current_time),
            'output': 'bridge/prediction/output/{}/{}/'.format(
                self.granularity_conf['mid'], current_time)
        }

        filename_tags_map = {}
        for metric in self.target_metrics:
            config = self.measurement_conf[metric].copy()
            config.update(self.granularity_conf)

            # [1] Retrieve data from influxdb
            queried_data = self._query_data(config, pod, target_labels)
            if not queried_data:
                self.log.debug('Pod "%s" query results in "%s" is empty; '
                               'thus not predicted\n',
                               pod, config['measurement'])
                continue
            self.log.info('Pod "{%s}" data in "%s" were queried.',
                          pod, config['measurement'])

            # [2] Group data to series
            series_map, time_scaling_sec, filename_tags_submap = \
                self._group_data_to_series_map(queried_data, config)
            filename_tags_map.update(filename_tags_submap)

            # [3] Impute missing series to zeros
            series_map = self._impute_missing_series(series_map, config)

            # [4] Export series to files
            self._export_input_files(series_map, file_folder_name['input'],
                                     config['name'])

        # [6] Conduct prediction for each series with sufficient data
        if os.path.exists(file_folder_name['input']):
            file_metrics = os.listdir(file_folder_name['input'])
        else:
            return

        input_file_list = []
        for folder in file_metrics:
            input_file_list += [
                os.path.join(file_folder_name['input'], folder, file_name)
                for file_name in os.listdir(
                    os.path.join(file_folder_name['input'], folder))]
        input_file_list = list(self._chunks(input_file_list, thread_num))

        if not input_file_list:
            return
        for subinput_files in input_file_list:
            filename_tags_submap = {
                os.path.basename(k): filename_tags_map[os.path.basename(k)]
                for k in subinput_files}

            p = Process(target=predict_by_series,
                        args=(self.log, self.measurement_conf,
                              self.granularity_conf, subinput_files,
                              filename_tags_submap, file_folder_name,
                              time_scaling_sec))
            p.start()
            p.join()

        # [7] write prediction result via GRPC client.
        input_file_list = [item for sublist in input_file_list
                           for item in sublist]
        output_file_list = []
        for file_name in input_file_list:

            # output file
            out_file_name = '{}.prdt'.format(file_name.replace(
                file_folder_name['input'], file_folder_name['output']))
            if os.path.exists(out_file_name):
                output_file_list.append(out_file_name)

        self.write_pod_data(pod, output_file_list,
                            filename_tags_map, time_scaling_sec)

        # [8] write recommendation result via GRPC client
        rc_result = get_mock_recommendation(pod)
        if rc_result is not None:
            self.dao.write_container_recommendation_result(rc_result)

        # [9] Delete files.
        if os.path.exists(file_folder_name['input']):
            shutil.rmtree(file_folder_name['input'], ignore_errors=True)
        if os.path.exists(file_folder_name['output']):
            shutil.rmtree(file_folder_name['output'], ignore_errors=True)

    def _chunks(self, _list, num):
        try:
            for i in range(0, len(_list), num):
                yield _list[i:i + num]
        except Exception as e:  # pylint: disable=W0703
            self.log.warning(sys.exc_info()[0])
            self.log.warning(e)

    def _export_input_files(self, series_map, input_file_folder, metric_name):
        for series in series_map.keys():
            file_dir = os.path.join(input_file_folder, metric_name)
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            file_name = os.path.join(file_dir, str(series))

            with open(file_name, 'a') as outfile:
                for point in series_map[series]:
                    outfile.write(point + '\n')

    def _impute_missing_series(self, series_map, config):
        field_str = ''
        for field in config['fields']:  # pylint: disable=W0612
            field_str += ',([\\d\\.\\e\\+\\-]*)'
        regex = '^(\\d+){}'.format(field_str)

        imputed_series_map = {}
        # Initial values correspond to re mached grop(0) and group(1),
        # respectively, where group(1) correspond to timestep index
        # of the point
        field_num = len(config['fields'])

        for series in series_map.keys():
            imputed_series_map[series] = []
            augmented_field_all_none = ['False', 'False']
            for field in config['fields']:
                augmented_field_all_none.append('True')

            for point in series_map[series]:
                match = re.search(regex, point)

                # There is timestep, in addition to the metrics/fields other
                # than time
                group_num = field_num + 1

                # Group(0) correspond to original string, and group(1)
                # corresponds to timestep
                for i in range(2, group_num + 1):
                    if not not match.group(i):
                        augmented_field_all_none[i] = 'False'

            regex_sub = '\\1'
            for i in range(2, group_num + 1):
                regex_sub += ',\\{}'.format(i)
            for point in series_map[series]:
                for i in range(2, group_num + 1):
                    if augmented_field_all_none[i]:
                        regex_sub.replace('\\{}'.format(i), '0')
                        point = re.sub(regex, regex_sub, point)

                imputed_series_map[series].append(point)
        return imputed_series_map

    @staticmethod
    def _get_granularity_sec(config):
        """Get granularity in seconds."""
        predict_granularity_sec = 0
        if config['data_granularity'] == '10m':
            predict_granularity_sec = 600
        elif config['data_granularity'] == '1h':
            predict_granularity_sec = 3600
        elif config['data_granularity'] == '6h':
            predict_granularity_sec = 21600
        elif config['data_granularity'] == '12h':
            predict_granularity_sec = 43200
        elif config['data_granularity'] == '1d':
            predict_granularity_sec = 86400
        elif config['data_granularity'] == '4s':
            predict_granularity_sec = 4
        elif config['data_granularity'] == '30s':
            predict_granularity_sec = 30
        else:
            raise ValueError('prediction granularity {} not implemented!\n'.
                             format(config['data_granularity']))

        return predict_granularity_sec

    def _group_data_to_series_map(self, queried_data, config):
        series_map = {}
        filename_tags_map = {}

        time_scaling_sec = self._get_granularity_sec(config)

        for container_data in queried_data:
            labels = container_data['labels']
            file_name = ''
            for tag in sorted(labels):
                tag_value = labels[tag]
                file_name += ',{}={}'.format(tag, tag_value)
            file_name += ',mid={}'.format(config['mid'])
            tags = file_name

            file_name = str(uuid.uuid3(uuid.NAMESPACE_DNS, file_name))
            filename_tags_map[file_name] = tags

            for point in container_data.get('data'):
                point_timestep = int(point.get('time') / time_scaling_sec)

                if file_name not in series_map:
                    series_map[file_name] = []

                point_str = str(point_timestep)
                for field in config['fields']:
                    field_value = point.get(field['name'])
                    if field_value is None:
                        field_value = ''
                    point_str += ',{}'.format(
                        ('', str(field_value))[not not str(field_value)])

                series_map[file_name].append(point_str)
        return series_map, time_scaling_sec, filename_tags_map

    # pylint: disable=W0613
    def _query_data(self, config, pod, target_labels=None, target_mid=None):

        self.log.debug('DAO get_container_observed_data INPUT: '
                       'metric_name=%s, namespace=%s, '
                       'pod_name=%s, data_amount=%d',
                       config['measurement'], pod['namespace'],
                       pod['pod_name'], config['data_amount_sec'])

        try:
            queried_result = self.dao.get_container_observed_data(
                config['measurement'], pod['namespace'], pod['pod_name'],
                config['data_amount_sec'])

            return queried_result
        except Exception as e:  # pylint: disable=W0703
            self.log.error(e)

    def write_pod_data(self, pod_info, output_file_list,
                       filename_tags_map, time_scaling_sec):
        ''' write the json data to alameda, convert infxludb posting
        data format into json(dict) object '''

        out_data = {
            'uid': pod_info.get('uid'),
            'namespace': pod_info.get('namespace'),
            'pod_name': pod_info.get('pod_name'),
            'containers': []
        }

        container_result = self.format_container_prediction_result(
            output_file_list, filename_tags_map, time_scaling_sec)
        if container_result:
            out_data['containers'] = list(container_result.values())
            self.log.debug('Write prediction data: %s', out_data)
            self.dao.write_container_prediction_data(out_data)

        return out_data

    def format_container_prediction_result(self, container_files,
                                           filename_tags_map,
                                           time_scaling_sec):

        """read data from every prediction file of the container.
           files are in the same container name, but different metric result;
           each file is in the same metric name."""
        container_set = dict()
        for file_path in container_files:
            metric_set = dict()
            data = get_csv_data(file_path)

            container_name = get_container_name(file_path, filename_tags_map)
            metric_name, config = get_metric_name_and_conf(
                file_path, self.measurement_conf)

            if container_name is None or metric_name is None:
                continue


            metric_set[metric_name] = []

            prdt_times = data[:, 0]
            prdt_values = data[:, 1:]
            for index_time, target_time in enumerate(prdt_times):
                data_pair = dict()
                data_pair['time'] = int(target_time * time_scaling_sec)

                for index_field in range(len(config['fields'])):
                    data_type = config['fields'][index_field]['data_type']
                    field_name = config['fields'][index_field]['name']
                    if data_type == 'i':
                        field_value = str(
                            int(round(prdt_values[index_time, index_field])))
                    else:
                        field_value = str(prdt_values[index_time, index_field])

                    data_pair[field_name] = field_value
                metric_set[metric_name].append(data_pair)

            if container_name not in container_set:
                container_set[container_name] = {
                    'container_name': container_name,
                    'raw_predict': metric_set
                }
            else:
                container_set[container_name]['raw_predict'].update(metric_set)

        return container_set
