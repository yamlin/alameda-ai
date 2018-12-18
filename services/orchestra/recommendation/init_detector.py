""" Init Detector """
import numpy as np
from framework.log.logger import Logger

class InitDetector:
    """ Detect the initial stage of a container workload """
    def __init__(self, logger=None):
        """ Initialize the InitDetector """
        self.MIN_SAMPLE_SIZE = 6
        self.logger = logger or Logger()

    def detect(self, data, neighbor_size=2):
        """ Do init detection.
        Args:
            data: an numpy array of row vector or column vector
            neighbor_size: the parameter for finding peaks of data
        Returns:
            results['init_mean']: the average value of the init stage
            results['init_max]: the maximum of the init stage
            results['first_peak_found']: true if the first peak of the data
                                         is found
            results['end_index']: the index of the end of the detected init
                                  stage; equals to -1 if first_peak_found is
                                  false
        """
        try:
            data_shape = data.shape
            if not (len(data_shape) == 1 or
                    (len(data_shape) == 2 and data_shape[1] == 1)):
                raise ValueError(
                    'input data must be a row vector or column vector')
        except ValueError as err:
            self.logger.error(err)
            return None
        except Exception as err:
            self.logger.error(err)
            self.logger.error(
                'fail to parsing shape of input data; '
                'be sure that the input data is an numpy array')
            return None

        if len(data) < self.MIN_SAMPLE_SIZE:
            self.logger.error('the number of input samples must not be less than 6')
            return None

        sample_num = len(data)
        first_peak_index = -1
        first_valley_index = -1
        first_peak_found = False

        # find the first peak
        for i in range(0, sample_num-neighbor_size):
            _beg = max(0, i-neighbor_size)
            _end = min(i+neighbor_size+1, sample_num)
            # if data[i] is peak
            if data[i] == np.max(data[_beg:_end]):
                first_peak_index = i
                first_peak_found = True
                break

        if first_peak_found:
            # find the first valley after the first peak
            for i in range(first_peak_index+1, sample_num-neighbor_size):
                _beg = max(first_peak_index, i-neighbor_size)
                _end = min(i+neighbor_size+1, sample_num)
                if data[i] == np.min(data[_beg:_end]):
                    first_valley_index = i
                    break
            if first_valley_index == -1:
                first_valley_index = min(2*first_peak_index, sample_num)
                if first_valley_index == first_peak_index:
                    first_valley_index = sample_num - 1

        if first_peak_found:
            init_mean = np.mean(data[:first_valley_index+1])
            init_max = data[first_peak_index]
        else:
            init_mean = np.mean(data)
            init_max = np.max(data)

        results = {'init_mean': init_mean,
                   'init_max': init_max,
                   'first_peak_found': first_peak_found,
                   'end_index': first_valley_index,
                   }

        return results
