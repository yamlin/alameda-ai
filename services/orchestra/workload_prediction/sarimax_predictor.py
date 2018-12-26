# -*- coding: utf-8 -*-
""" SARIMAX predictor """
# pylint: disable=E0401
# pylint: disable=C0103
import itertools
import os
import yaml
import numpy as np
import statsmodels.api as sm
from sklearn.preprocessing import MinMaxScaler
from framework.log.logger import Logger
from services.orchestra.workload_prediction.preprocessor import Preprocessor


class SARIMAXPredictor:
    """The SARIMAX predictor"""

    def __init__(self, log=None, config_file=None):
        """Initialize the predictor.

        Args:
            config_file: configuration file of parameters

        Returns: none
        """

        self.TIMES = 'times'
        self.VALUES = 'values'
        self.ORDER = 'order'
        self.SEASONAL_ORDER = 'seasonal_order'

        self.MIN_OBSERVATION_NUM = 12
        self.MAX_SEANSONALITY = 32
        self.MIN_SEANSONALITY = 4

        # Used to avoid 'maxlag should be < nobs' error that occurs with
        self.OBSERVATION_MULTIPLE = 6

        # The latest data samples for trend detection
        self.TREND_DATA_SIZE = 100

        # Threshold of correlation coefficient for trend detection
        self.TREND_CORRCOEF = 0.4

        # feature_range of MinMaxScalar
        # not starting from 0 to avoid that data samples are all zeros
        self.minmax_norml_range = (1, 10)

        self.log = log or Logger()

        # Get parameters from configuration file.
        if config_file is None:
            config_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                'config/sarimax_config.yaml')

        try:
            with open(config_file, 'r') as stream:
                self.cfg = yaml.load(stream)
        except FileNotFoundError:
            self.log.error("SARIMAX predictor's configure file not found.")
            raise

    def predict(self, observed_data, predict_steps, normalization=True):
        """ Make predictions
        Args:
            observed_data: {
                'times': x,
                'values': y,
            }, where x= [i, i+1, i+2,…, i+n], and i denotes the ith time step;
                     y =[O(i), O(i+1), O(i+2),…,O(i+n)] is a numpy array
                     (row vector or column vector), where O(t) denotes the
                     observation at time step t.
            predict_steps: indicate predicting how many time steps
            normalization: ture if MinMax scaling is applied

        Return:
            predicted_data with the same data format as observed_data

        """

        if len(observed_data[self.VALUES]) < self.MIN_OBSERVATION_NUM:
            self.log.error(
                'The number of observations is at least {}'.format(
                    self.MIN_OBSERVATION_NUM))
            return None

        # Impute missing values
        pre = Preprocessor()
        observed_data[self.VALUES] = pre.impute_missing_value(
            observed_data[self.VALUES])
        observed_data[self.TIMES] = np.array(
            observed_data[self.TIMES], dtype='int64')

        if normalization:
            # Mix-Max scaling
            scaler = MinMaxScaler(feature_range=self.minmax_norml_range)
            scaler.fit(observed_data[self.VALUES])
            observed_data[self.VALUES] = scaler.transform(
                observed_data[self.VALUES])

        # Detect the seasonality
        seasonality, seasonality_found = self._ac_win_size(
            observed_data[self.VALUES])
        if seasonality_found:
            self.cfg[self.SEASONAL_ORDER]['s'] = min(
                seasonality, self.MAX_SEANSONALITY)
        else:
            self.cfg[self.SEASONAL_ORDER]['s'] = self.MIN_SEANSONALITY

        # To avoid 'maxlag should be < nobs' error that occurs with
        # insufficient data
        if len(observed_data[self.VALUES]) < (
                self.cfg[self.SEASONAL_ORDER]['s']*self.OBSERVATION_MULTIPLE):
            self.cfg[self.SEASONAL_ORDER]['s'] = \
                int(len(observed_data[self.VALUES]) / self.OBSERVATION_MULTIPLE)

        # Detect the trend
        sample_num = len(observed_data[self.VALUES])
        trend_values = observed_data[self.VALUES][
            -min(self.TREND_DATA_SIZE, sample_num):]
        trend_indeice = np.array(range(0, len(trend_values)))
        corrcoeffs = np.corrcoef(
            trend_indeice.reshape(1, -1), trend_values.reshape(1, -1))

        # If there is trend of increasing or decreasing
        if abs(corrcoeffs[0, 1]) > self.TREND_CORRCOEF:
            self.cfg[self.ORDER]['d'] = [1]
        else:
            self.cfg[self.ORDER]['d'] = [0]

        # Make prediction with grid search of SARIMAX
        try:
            # Generate all different combinations of p, q and q triplets
            pdq = list(itertools.product(
                self.cfg[self.ORDER]['p'],
                self.cfg[self.ORDER]['d'], self.cfg[self.ORDER]['q']))
            PDQ = list(itertools.product(
                self.cfg[self.SEASONAL_ORDER]['P'],
                self.cfg[self.SEASONAL_ORDER]['D'],
                self.cfg[self.SEASONAL_ORDER]['Q']))

            # Generate all different combinations of
            # seasonality, P, D and Q triplets
            seasonality_PDQ = [
                (x[0], x[1], x[2], self.cfg[
                    self.SEASONAL_ORDER]['s']) for x in PDQ]

            # Grid search
            AIC = []
            SARIMAX_model = []
            for param in pdq:
                for param_seasonal in seasonality_PDQ:
                    try:
                        mod = sm.tsa.statespace.SARIMAX(
                            observed_data[self.VALUES],
                            order=param,
                            seasonal_order=param_seasonal,
                            enforce_stationarity=False,
                            enforce_invertibility=False)

                        results = mod.fit(disp=0)

                        AIC.append(results.aic)
                        SARIMAX_model.append([param, param_seasonal])
                    except Exception as err:  # pylint: disable=W0703
                        self.log.info(err)
                        continue

            mod = sm.tsa.statespace.SARIMAX(
                observed_data[self.VALUES],
                order=SARIMAX_model[AIC.index(min(AIC))][0],
                seasonal_order=SARIMAX_model[AIC.index(min(AIC))][1],
                enforce_stationarity=False,
                enforce_invertibility=False)

            mdl_fit = mod.fit(disp=0)

            pred_temp = mdl_fit.get_forecast(steps=predict_steps)
            predictions = pred_temp.predicted_mean
        except np.linalg.linalg.LinAlgError:
            self.log.debug("Variance of observed data is: %s",
                           np.var(observed_data[self.VALUES]))
            predictions = np.mean(observed_data[self.VALUES], axis=0)
            predictions = np.tile(predictions, (predict_steps, 1))
        except Exception as err:  # pylint: disable=W0703
            self.log.error(err)
            return None

        if normalization:
            predicted_data = {
                self.VALUES: scaler.inverse_transform(
                    predictions.reshape(-1, 1)),
                self.TIMES: observed_data[self.TIMES][-1] + range(
                    predict_steps) + 1
            }
        else:
            predicted_data = {
                self.VALUES: predictions.reshape(-1, 1),
                self.TIMES: observed_data[self.TIMES][-1] + range(
                    predict_steps) + 1
            }

        return predicted_data

    def _ac_win_size(self, data):
        _, fea_num = data.shape
        win_size_list = []
        for i in range(fea_num):
            ac = self._acf(data[:, i])
            peak_index, _ = self._peak_ac_index(ac)
            # the index starts from 0
            w_size = peak_index + 1
            win_size_list.append(w_size)

        max_win_size = max(win_size_list)
        found = False
        if max_win_size > 0:
            found = True

        return max_win_size, found

    def _acf(self, data):

        y = data - np.mean(data)
        norm = np.sum(y ** 2)
        if norm == 0:
            return None
        ac = np.correlate(y, y, mode='full') / norm
        ac = ac[int(ac.size / 2.0):]
        return ac

    def _peak_ac_index(self, ac, neighbor_size=4):

        if ac is None or len(ac) < (2 * neighbor_size + 1):
            max_peak = -1
            max_peak_index = -1
            return max_peak_index, max_peak

        max_peak = -1
        max_peak_index = -1
        for i in range(neighbor_size, len(ac) - neighbor_size):
            # if ac[ac] is peak
            if ac[i] == max(
                    ac[i - neighbor_size:i + neighbor_size]):
                if ac[i] > max_peak:
                    max_peak = ac[i]
                    max_peak_index = i
        # ignore the negative correlation case
        if max_peak < 0.1:
            max_peak_index = -1
        return max_peak_index, max_peak
