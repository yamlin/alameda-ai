# -*- coding: utf-8 -*-
""" Process threading """
# pylint: disable=E0401
import os
import threading
import numpy as np

from services.arima.workload_prediction.sarimax_predictor \
    import SARIMAXPredictor
from services.arima.workload_prediction.workload_utils \
    import get_csv_data


# Minimum sample number for both model training and prediction
MIN_SAMPLE_SIZE = 6

# Prediction steps is the multiplication of input sample size and
# PREDICT_DATA_RATE
PREDICT_DATA_RATE = 0.5


class PredictionThread(threading.Thread):
    """ Thread for prediction """
    def __init__(self, target_fun, log, predictor, observed_data,
                 file_name, output_file_folder,
                 config, time_scaling_ns, filename_tags_map):
        threading.Thread.__init__(self)
        self.log = log
        self.target_fun = target_fun
        self.predictor = predictor
        self.observed_data = observed_data
        self.file_name = file_name
        self.output_file_folder = output_file_folder
        self.config = config
        self.time_scaling_ns = time_scaling_ns
        self.filename_tags_map = filename_tags_map

    def run(self):
        self.target_fun(self.log, self.predictor, self.observed_data,
                        self.file_name, self.output_file_folder, self.config)


def predict_by_series(log, measurement_conf, granularity_conf, input_file_list,
                      filename_tags_map, file_folder_name, time_scaling_ns):
    """Predict by identical device"""

    thread_pool = []
    filename_tags_map_o = filename_tags_map.copy()
    file_folder_name = file_folder_name.copy()
    for file_path in input_file_list:
        ignored = False

        file_name = file_path.replace(file_folder_name['input'], '')
        metric_name, file_name = os.path.split(file_name)
        config = measurement_conf[metric_name].copy()
        config.update(granularity_conf)

        file_folder_name['model'] = \
            'models/online/workload_prediction/{}/{}/{}'.format(
                config['mid'], config['name'], file_name)
        log.info("Prediction: %s", file_folder_name['model'])

        observed_data = get_csv_data(file_path)
        # pylint: disable=W0612
        config['sample_size'], _ = observed_data.shape
        # pylint: enable=W0612
        observed_data = {
            'times': observed_data[:, 0],
            'values': observed_data[:, 1:],
        }

        config_prdt = _get_prediction_conf(config)

        if 'minimal_sample_size' in config.keys():
            if config['sample_size'] < MIN_SAMPLE_SIZE:
                ignored = True
                log.warning(
                    'number of data sample of %s less than %d, thus not '
                    'predicted\n', file_folder_name['model'], MIN_SAMPLE_SIZE)
            else:
                if config['sample_size'] < \
                        config['minimal_sample_size'] - 1:
                    filename_tags_map[file_name] = '{}_ini'.format(
                        filename_tags_map_o[file_name])

                    log.warning(
                        'number of data sample of %s is %d and less than '
                        'minimal_sample_size(%s), thus may result in poor '
                        'prediction\n',
                        file_folder_name['model'], config['sample_size'],
                        config['minimal_sample_size'])
        else:
            raise ValueError(
                'minimal_sample_size not defined in {}\n'.format(
                    config['name']))

        if not ignored:
            predictor = SARIMAXPredictor(log=log)

            prediction_thread = PredictionThread(
                _predict_write_file, log, predictor, observed_data, file_name,
                file_folder_name['output'], config_prdt,
                time_scaling_ns, filename_tags_map)
            thread_pool.append(prediction_thread)

    if thread_pool:
        for thread in thread_pool:
            thread.start()
        for thread in thread_pool:
            thread.join()


def _get_prediction_conf(config):
    config_copy = config.copy()
    if 'prediction_steps' not in config.keys():
        config_copy['prediction_steps'] = int(
            config['sample_size'] * PREDICT_DATA_RATE)
    else:
        config_copy['prediction_steps'] = min(
            config['prediction_steps'],
            int(config['sample_size'] * PREDICT_DATA_RATE))

    return config_copy


def _diff_values(values):
    num_sample, num_fea = values.shape
    diffs = values[1:, 0] - values[:num_sample - 1, 0]
    diffs = diffs.reshape(num_sample - 1, 1)
    for i in range(num_fea):
        if i > 0:
            temp = values[1:, i] - values[:num_sample - 1, i]
            diffs = np.append(diffs, temp.reshape(num_sample - 1, 1),
                              axis=1)
    return diffs


def _get_diff_data(observed_data):
    data_copy = observed_data.copy()
    data_copy['values'] = _diff_values(data_copy['values'])
    data_copy['times'] = observed_data['times'][1:]

    return data_copy


def _predict_write_file(log, predictor, observed_data,
                        file_name, output_file_folder, config):

    # pylint: disable=W0612
    if 'diff' in config.keys() and config['diff']:
        diffs = _diff_times_values(observed_data.copy())
        predicted_data = _predict_diff(predictor,
                                       observed_data.copy(),
                                       diffs.copy(),
                                       config['prediction_steps'])
        if not predicted_data:
            return

        prdt_times = predicted_data['times']
        prdt_values = predicted_data['values']
        if np.isnan(prdt_values).any():
            log.warning("Predicted data exists NaN, thus not "
                        "writing predicted data onto DB.\n")
            return
    else:
        predicted_data = predictor.predict(
            observed_data.copy(), config['prediction_steps'])
        if not predicted_data:
            return

        prdt_times = predicted_data['times']
        prdt_values = predicted_data['values']
        if np.isnan(prdt_values).any():
            log.warning("Predicted data exists NaN, thus not "
                        "writing predicted data onto DB.\n")
            return

        # Prediction value lower bound should be 0.
        prdt_values = np.maximum(prdt_values, 0)

    # Write prediction data to file
    out_prdt_file = '{}{}/{}.prdt'.format(
        output_file_folder, config['name'], file_name)
    if not os.path.exists(os.path.dirname(out_prdt_file)):
        try:
            os.makedirs(os.path.dirname(out_prdt_file))
        except FileExistsError:
            pass

    with open(out_prdt_file, "a") \
            as outfile:

        for index_time, target_time in enumerate(prdt_times):
            predict_point = str(int(target_time))

            for index_field in range(len(config['fields'])):
                field_value = str(prdt_values[index_time, index_field])
                predict_point += ',{}'.format(
                    ('', str(field_value))[not not str(field_value)])

            outfile.write(predict_point + '\n')


def _diff_times_values(observed_data):
    num_sample, num_fea = observed_data['values'].shape
    diffs = \
        observed_data['values'][1:, 0] - \
        observed_data['values'][:num_sample - 1, 0]
    diffs = diffs.reshape(num_sample - 1, 1)
    for i in range(num_fea):
        if i > 0:
            temp = observed_data['values'][1:, i] - \
                   observed_data['values'][:num_sample - 1, i]
            diffs = np.append(diffs, temp.reshape(num_sample - 1, 1), axis=1)

    observed_diffs = {'times': observed_data['times'][1:],
                      'values': diffs}

    return observed_diffs


def _predict_diff(predictor, observed_data, observed_diffs,
                  prediction_steps):
    num_sample, num_fea = observed_data['values'].shape
    # pylint: disable=W0612
    predicted_diffs = predictor.predict(
        observed_diffs.copy(), prediction_steps)
    if not predicted_diffs:
        return predicted_diffs
    # pylint: enable=W0612

    min_diff = 0
    negatve_index = np.where(predicted_diffs['values'] < min_diff)
    predicted_diffs['values'][negatve_index] = 0

    predicted_data = {'times': predicted_diffs['times'],
                      'values': np.array([[]])}
    predicted_data['values'] = \
        observed_data['values'][num_sample - 1, :] + \
        predicted_diffs['values'][0, :]
    predicted_data['values'] = predicted_data['values'].reshape(1, num_fea)
    for i in range(prediction_steps):
        if i > 0:
            temp = predicted_data['values'][i - 1, :] + \
                   predicted_diffs['values'][i, :]
            predicted_data['values'] = np.append(predicted_data['values'],
                                                 temp.reshape(1, num_fea),
                                                 axis=0)
    return predicted_data
