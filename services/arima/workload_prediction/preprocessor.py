# -*- coding: utf-8 -*-
""" For Preprocessing of data before entering the predictor """
# pylint: disable=E0401
import numpy as np
from sklearn.preprocessing import Imputer


class Preprocessor:
    """preprocessing data to avoid numerical errors"""

    def __init__(self):
        return

    def add_some_smallvalues_if_zerovar(self, data, add_amount=0, add_value=0):
        """ add small values for each column of data whose variance
        is zero """
        num_samples, num_fea = data.shape

        if num_samples < add_amount:
            add_amount = num_samples
        for i in range(num_fea):
            if np.var(data[:, i], axis=0) == 0:
                add_index = np.floor(np.random.rand(add_amount)*num_samples)
                # pylint: disable=W0612
                for idx, j in np.ndenumerate(add_index):
                    data[int(j), i] = data[int(j), i] + add_value
        return data

    def impute_missing_value(self, data):
        """ Impute missing values using sklearn

        Args:
            data: an numpy array
        Returns: data where missing values been imputed
        """
        # Missing value is '' in the array
        # index_null = np.where('' == data)

        # If there is missing value
        # if index_null[0].size > 0:
        data_dtype = str(data.dtype)
        if data_dtype.find('float') == -1 and data_dtype.find('int') == -1:
            # Replace '' with np.nan
            np.place(data, data == '', np.nan)

            data = np.array(data, dtype='float32')

            # Create the imputer
            imp = Imputer(missing_values=np.nan, strategy='mean', axis=0)

            # Train/fit the imputer
            imp.fit(data)

            # Impute missing values in data
            data = imp.transform(data)

        return data
