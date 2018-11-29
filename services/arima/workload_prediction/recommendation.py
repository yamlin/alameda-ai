# -*- coding: utf-8 -*-
""" Recommendation """


def get_mock_recommendation(pod_info):
    """Get mock recommendation result"""

    policy = pod_info.get("policy")
    if policy == "COMPACT":
        return {"uid": "5bda5c7f-a176-3549-98a6-8d9fb6fc13e4",
                "namespace": "default",
         "pod_name": "router-2-m7vx8", "containers": [
            {"container_name": "router2", "recommendations": [
                {"time": 1540973220,
                 "resources": {"limits": {"cpu": "150m", "memory": "0"},
                               "requests": {"cpu": "141m", "memory": "0"}}},
                {"time": 1540973460,
                 "resources": {"limits": {"cpu": "150m", "memory": "0"},
                               "requests": {"cpu": "144m", "memory": "0"}}},
                {"time": 1540973670,
                 "resources": {"limits": {"cpu": "150m", "memory": "0"},
                               "requests": {"cpu": "145m", "memory": "0"}}}],
             "init_resource": {"limits": {"cpu": "150m", "memory": "0"},
                               "requests": {"cpu": "141m", "memory": "0"}}},
            {"container_name": "router1", "recommendations": [
                {"time": 1540973220,
                 "resources": {"limits": {"cpu": "150m", "memory": "0"},
                               "requests": {"cpu": "141m", "memory": "0"}}},
                {"time": 1540973460,
                 "resources": {"limits": {"cpu": "150m", "memory": "0"},
                               "requests": {"cpu": "147m", "memory": "0"}}},
                {"time": 1540973670,
                 "resources": {"limits": {"cpu": "150m", "memory": "0"},
                               "requests": {"cpu": "144m", "memory": "0"}}}],
             "init_resource": {"limits": {"cpu": "150m", "memory": "0"},
                               "requests": {"cpu": "141m", "memory": "0"}}}]}
    else:
        return {"uid": "5bda5c7f-a176-3549-98a6-8d9fb6fc13e4",
                "namespace": "default",
                "pod_name": "router-2-m7vx8", "containers": [
                {"container_name": "router2", "recommendations": [
                    {"time": 1540973220,
                     "resources": {"limits": {"cpu": "150m", "memory": "0"},
                                   "requests": {"cpu": "141m",
                                                "memory": "0"}}}],
                 "init_resource": {"limits": {"cpu": "150m", "memory": "0"},
                                   "requests": {"cpu": "141m", "memory": "0"}}},
                {"container_name": "router1", "recommendations": [
                    {"time": 1540973220,
                     "resources": {"limits": {"cpu": "150m", "memory": "0"},
                                   "requests": {"cpu": "141m",
                                                "memory": "0"}}}],
                 "init_resource": {"limits": {"cpu": "150m", "memory": "0"},
                                   "requests": {"cpu": "141m",
                                                "memory": "0"}}}]}
