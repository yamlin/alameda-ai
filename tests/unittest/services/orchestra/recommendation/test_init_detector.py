""" Unit test for init detector"""
import unittest
import numpy as np
from services.orchestra.recommendation.init_detector import InitDetector


class InitDetectorTestCase(unittest.TestCase):
    """ Test cases for init detector  """
    def setUp(self):
        self.data_case1 = np.array(
            [0.24, 0.38, 0.51, 0.38, 0.31, 0.3, 0.32, 0.24, 0.37, 0.29])
        self.init_mean_case1 = 0.34
        self.init_max_case1 = 0.51
        self.first_peak_found_case1 = True
        self.end_index_case1 = 7

        self.data_case2 = np.array(
            [0.13, 0.03, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.02, 0.02]
        ).reshape(-1, 1)
        self.init_mean_case2 = 0.06
        self.init_max_case2 = 0.13
        self.first_peak_found_case2 = True
        self.end_index_case2 = 2

        self.data_case3 = np.array(
            [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])
        self.init_mean_case3 = 0.55
        self.init_max_case3 = 1.0
        self.first_peak_found_case3 = False
        self.end_index_case3 = -1

        self.data_case4 = np.array(
            [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1])
        self.init_mean_case4 = 0.55
        self.init_max_case4 = 1.0
        self.first_peak_found_case4 = True
        self.end_index_case4 = 9

    def tearDown(self):
        pass

    def test_detect_case1(self):
        init_detector = InitDetector()
        results = init_detector.detect(self.data_case1)
        self.assertTupleEqual(
            (self.init_mean_case1, self.init_max_case1,
             self.first_peak_found_case1, self.end_index_case1),
            (np.round(results['init_mean'], 2), np.round(results['init_max'], 2),
             results['first_peak_found'], results['end_index']))

    def test_detect_case2(self):
        init_detector = InitDetector()
        results = init_detector.detect(self.data_case2)
        self.assertTupleEqual(
            (self.init_mean_case2, self.init_max_case2,
             self.first_peak_found_case2, self.end_index_case2),
            (np.round(results['init_mean'], 2), np.round(results['init_max'], 2),
             results['first_peak_found'], results['end_index']))

    def test_detect_case3(self):
        init_detector = InitDetector()
        results = init_detector.detect(self.data_case3)
        self.assertTupleEqual(
            (self.init_mean_case3, self.init_max_case3,
             self.first_peak_found_case3, self.end_index_case3),
            (np.round(results['init_mean'], 2), np.round(results['init_max'], 2),
             results['first_peak_found'], results['end_index']))

    def test_detect_case4(self):
        init_detector = InitDetector()
        results = init_detector.detect(self.data_case4)
        self.assertTupleEqual(
            (self.init_mean_case4, self.init_max_case4,
             self.first_peak_found_case4, self.end_index_case4),
            (np.round(results['init_mean'], 2), np.round(results['init_max'], 2),
             results['first_peak_found'], results['end_index']))


if __name__ == '__main__':
    unittest.main()
