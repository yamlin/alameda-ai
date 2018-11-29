# -*- coding: utf-8 -*-
""" Check license by host. """
# pylint: disable=E0401
import os
import csv
import numpy as np


def get_csv_data(csv_file):
    """Get observed data in csv format."""
    data = []
    with open(csv_file, 'r') as csv_fid:
        reader = csv.reader(csv_fid)
        for row in reader:
            # print (row)
            r_list = []
            for cell in row:
                if cell != '':
                    r_list.append(float(cell))
                else:
                    r_list.append('')

            r_list[0] = int(r_list[0])
            data.append(r_list)

    return np.array(data)


def get_container_name(file_path, filename_tags_map):
    """Get container name from tags."""
    file_name = os.path.basename(file_path)
    if ".prdt" in file_name:
        file_name = file_name.replace(".prdt", "")

    tags = filename_tags_map[file_name]
    tags = dict(t.split('=') for t in tags.split(',') if '=' in t)
    container_name = tags.get("container_name")

    return container_name


def get_metric_name_and_conf(file_path, measurement_conf):
    """Get metric name and its configure."""
    config = dict()
    metric_name = None
    for name in measurement_conf:
        if name in file_path:
            config = measurement_conf[name]
            metric_name = config.get("measurement")
            break

    return metric_name, config
