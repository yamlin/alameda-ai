""" Unit test for sarimax predictor """
import unittest
import numpy as np
from services.orchestra.workload_prediction.sarimax_predictor import SARIMAXPredictor

class SARIMAXPredictorTestCase(unittest.TestCase):
    """ Test cases for sarimax predictor """
    def setUp(self):
        self.observed_data = {}
        self.observed_data['times'] = np.array([0, 1, 2, 3, 4, 5])
        self.observed_data['values'] = np.array([202, 597, 108, 710, 879, 970])
        self.predict_steps = 3

    def tearDown(self):
        pass

    def test_too_less_obserivations(self):
        """ Test the case of too less observations """
        config_file = './services/orchestra/workload_prediction/config/sarimax_config.yaml'
        predictor = SARIMAXPredictor(config_file=config_file)
        predicted_data = predictor.predict(
            self.observed_data, self.predict_steps)
        self.assertEqual(predicted_data, None)


if __name__ == '__main__':
    unittest.main()
