# -*- coding: utf-8 -*-
""" SARIMAX predictor """
# pylint: disable=E0401
import os
import yaml
import numpy as np
import statsmodels.api as sm

from framework.log.logger import Logger
from services.arima.workload_prediction.preprocessor import Preprocessor


OBSERVATION_MULTIPLE = 3
MIN_OBSERVATION_NUM = 6


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

    def predict(self, observed_data, predict_steps):
        """ Make predictions
        Args:
            observed_data: {
                'times': x,
                'values': y,
            }, where x= [i, i+1, i+2,…, i+n], and i denotes the ith time step;
                     y =[
                            [fea_0(i), fea_1(i), fea_2(i),…,fea_k(i)],
                            [fea_0(i+1), fea_1(i+1), fea_2(i+1),…,fea_k(i+1)],
                             …
                            [fea_0(i+n), fea_1(i+n), fea_2(i+n),…,fea_k(i+n)],
                        ], where fea_j(t) denotes the value of the jth feature
                        at time step t.
            predict_steps: indicate predicting how many time steps

        Return:
            predicted_data with the same data format as observed_data

        """

        # Impute missing values
        pre = Preprocessor()
        observed_data[self.VALUES] = pre.impute_missing_value(
            observed_data[self.VALUES])
        observed_data[self.TIMES] = np.array(
            observed_data[self.TIMES], dtype='int64')

        # To avoid 'maxlag should be < nobs' error
        if len(observed_data[self.VALUES]) < (
                self.cfg[self.SEASONAL_ORDER]['s']*OBSERVATION_MULTIPLE):
            self.cfg[self.SEASONAL_ORDER]['s'] = \
                int(len(observed_data[self.VALUES]) / OBSERVATION_MULTIPLE)

        # Make prediction with SARIMAX
        try:
            model = sm.tsa.statespace.SARIMAX(
                observed_data[self.VALUES],
                order=(self.cfg[self.ORDER]['p'], self.cfg[self.ORDER]['d'],
                       self.cfg[self.ORDER]['q']),
                seasonal_order=(
                    self.cfg[self.SEASONAL_ORDER]['P'],
                    self.cfg[self.SEASONAL_ORDER]['D'],
                    self.cfg[self.SEASONAL_ORDER]['Q'],
                    self.cfg[self.SEASONAL_ORDER]['s']),
                enforce_stationarity=False,
                enforce_invertibility=False)

            mdl_fit = model.fit(disp=0)
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

        predicted_data = {
            self.VALUES: predictions.reshape(-1, 1),
            self.TIMES: observed_data[self.TIMES][-1] + range(predict_steps) + 1
        }

        return predicted_data
