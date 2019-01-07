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
from services.orchestra.recommendation.data_processor import DataProcessor
from services.orchestra.workload_prediction.process_threading \
    import predict_by_series
from services.orchestra.workload_prediction.preprocessor import Preprocessor
from services.orchestra.workload_prediction.workload_utils \
    import get_csv_data, get_unit_name_type


class WorkloadPredictor:
    """ Workload predictor """

    def __init__(self, log=None, processor=None, preprocesser=None):
        # Max filename length of linux is 255;
        # reserve capacity 20 character for further name appending
        # e.g., .prdt_log in _predict_write_influx()
        self.MAX_FILENAME_LENGTH = 235
        self.APP_PATH = os.path.dirname(os.path.abspath(__file__))

        with open(os.path.join(
                self.APP_PATH, 'config/granularity_conf.yaml')) as yaml_file:
            granularity_conf = yaml.load(yaml_file)
            granularity_conf = granularity_conf['30s']
        self.granularity_conf = granularity_conf

        self.log = log or Logger(name='workload_prediction',
                                 logfile='/var/log/workload_prediction.log',
                                 level=LogLevel.LV_DEBUG)
        self.processor = processor or DataProcessor(logger=self.log)
        self.preprocessor = preprocesser or Preprocessor()

    def predict(self, unit, unit_type, thread_num=1, target_labels=None):
        """ Prediction
        :param unit: (dict) the info of the unit that need train/predict.
        :param unit_type: (str) 'POD' or 'NODE'.
        :param thread_num: the number of threading.
        :param target_labels: (list) target labels.
        """

        current_time = datetime.now().strftime('%Y%m%d%H%M%S')
        file_folder_name = {
            'input': 'bridge/prediction/input/{}/{}/{}/'.format(
                self.granularity_conf['mid'], unit_type, current_time),
            'output': 'bridge/prediction/output/{}/{}/{}/'.format(
                self.granularity_conf['mid'], unit_type, current_time)
        }

        if not os.path.exists(file_folder_name['input']):
            os.makedirs(file_folder_name['input'])
        if not os.path.exists(file_folder_name['output']):
            os.makedirs(file_folder_name['output'])

        filename_tags_map = {}
        config = self.granularity_conf.copy()

        # [1] Retrieve data from influxdb
        queried_data = self._query_data(unit, unit_type, target_labels)
        if not queried_data:
            self.log.debug('Predict unit %s "%s" query results is empty; '
                           'thus not predicted\n', unit_type, unit)
            return
        self.log.info('Predict unit %s "%s" data were queried.',
                      unit_type, unit)

        # [2] Group data to series
        series_map, _, filename_tags_submap = \
            self._group_data_to_series_map(queried_data, config)
        filename_tags_map.update(filename_tags_submap)

        # [3] Impute missing series to zeros
        # series_map = self._impute_missing_series(series_map, config)

        # [4] Export series to files
        self._export_input_files(series_map, file_folder_name['input'])

        # [6] Conduct prediction for each series with sufficient data
        input_file_list = os.listdir(file_folder_name['input'])
        input_file_list = list(self._chunks(input_file_list, thread_num))
        for subinput_files in input_file_list:
            filename_tags_submap = {
                os.path.basename(k): filename_tags_map[os.path.basename(k)]
                for k in subinput_files}

            p = Process(target=predict_by_series,
                        args=(self.log, self.granularity_conf, subinput_files,
                              filename_tags_submap, file_folder_name))
            p.start()
            p.join()

        # [7] write prediction result via GRPC client.
        input_file_list = [item for sublist in input_file_list
                           for item in sublist]
        output_file_list = []
        for file_name in input_file_list:

            # output file
            out_file_name = '{}.prdt'.format(
                os.path.join(file_folder_name['output'], file_name))
            if os.path.exists(out_file_name):
                output_file_list.append(out_file_name)

        self.write_predict_data(unit, unit_type, output_file_list,
                                filename_tags_map)

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

    @staticmethod
    def _export_input_files(series_map, input_file_folder):
        for series in series_map.keys():
            file_name = os.path.join(input_file_folder, str(series))
            with open(file_name, 'a') as outfile:
                for point in series_map[series]:
                    outfile.write(point + '\n')

    @staticmethod
    def _impute_missing_series(series_map, config):
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

        for name, metric_data in queried_data.items():
            for metric_type, data in metric_data.items():
                file_name = 'name={},type={},mid={}'.format(
                    name, metric_type, config['mid'])
                tags = file_name

                file_name = str(uuid.uuid3(uuid.NAMESPACE_DNS, file_name))
                filename_tags_map[file_name] = tags

                series_map[file_name] = \
                    ['%s,%s' % (key, val) for key, val in data.items()]

        return series_map, time_scaling_sec, filename_tags_map

    # pylint: disable=W0613
    def _query_data(self, unit, unit_type, target_labels=None, target_mid=None):

        try:
            if unit_type == 'POD':
                queried_result = self.processor.query_containers_observed_data(
                    unit)

            elif unit_type == 'NODE':
                queried_result = self.processor.query_nodes_observed_data(
                    [unit])
            else:
                raise NameError(unit_type, "Predict unit type is not defined.")
            self.log.debug("Queried workload data: %s", queried_result)
            return queried_result
        except KeyError as err:
            self.log.error("Error in query data: %s %s", type(err), str(err))
            self.log.error("There might be a mismatch in predict unit info: "
                           "%s", unit)
        except Exception as err:  # pylint: disable=broad-except
            self.log.error("Error in query data: %s %s", type(err), str(err))

    def write_predict_data(self, unit, unit_type, output_file_list,
                           filename_tags_map):
        ''' write the json data to alameda, convert influxdb posting
        data format into json(dict) object '''

        try:
            if unit_type == 'POD':
                container_result = self.format_prediction_result(
                    output_file_list, filename_tags_map)
                if container_result:
                    self.processor.write_containers_predicted_data(
                        unit, container_result)

            elif unit_type == 'NODE':
                node_result = self.format_prediction_result(
                    output_file_list, filename_tags_map)

                if node_result:
                    self.processor.write_nodes_predicted_data(node_result)

            else:
                raise NameError(unit_type, "Predict unit type is not defined.")
        except Exception as err:  # pylint: disable=W0703
            self.log.error("Write POD prediction error: %s %s in {%s}",
                           type(err), str(err), unit)

    @staticmethod
    def format_prediction_result(container_files, filename_tags_map):

        """read data from every prediction file of identical predicted unit,
        and format the predicted data to the dictionary structure
        used in data processor."""
        unit_set = dict()
        for file_path in container_files:
            data = get_csv_data(file_path)

            unit_name, metric_type = \
                get_unit_name_type(file_path, filename_tags_map)

            if unit_name is None or metric_type is None:
                continue

            prdt_times = data[:, 0]
            prdt_values = data[:, 1]

            data_pair = dict()
            for time, value in zip(prdt_times, prdt_values):
                data_pair.update({int(time): value})
            metric_set = {metric_type: data_pair}

            if metric_set:
                if unit_name in unit_set:
                    unit_set[unit_name].update(metric_set)
                else:
                    unit_set[unit_name] = metric_set

        return unit_set
