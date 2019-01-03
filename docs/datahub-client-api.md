# Method get_data(data_type, args)

### container_init
##### args type: ListPodMetricsRequest
``` json
{
	"namespaced_name": {
		"namespace": "ns",
		"name": "name"
	},
	"time_range": {
		"start_time": "1970-01-01T00:00:00Z",
		"end_time": "1970-01-01T00:00:00Z",
		"step": 10
	}
}
```

### container_observed
##### args type: ListPodMetricsRequest
```json
{
	"namespaced_name": {
		"namespace": "ns",
		"name": "name"
	},
	"time_range": {
		"start_time": "1970-01-01T00:00:00Z",
		"end_time": "1970-01-01T00:00:00Z",
		"step": 10
	}
}
```

### container_predicted
##### args type: ListPodPredictionsRequest
```json
{
	"namespaced_name": {
		"namespace": "ns",
		"name": "name"
	},
	"time_range": {
		"start_time": "1970-01-01T00:00:00Z",
		"end_time": "1970-01-01T00:00:00Z",
		"step": 10
	}
}
```

### node_predicted
##### args type: ListNodePredictionsRequest
```json
{
	"node_name": ["node1", "node2"],
	"time_range": {
		"start_time": "1970-01-01T00:00:00Z",
		"end_time": "1970-01-01T00:00:00Z",
		"step": 10
	}
}
```

### node_observed
##### args type: ListNodeMetricsRequest
```json
{
	"node_name": ["node1", "node2"],
	"time_range": {
		"start_time": "1970-01-01T00:00:00Z",
		"end_time": "1970-01-01T00:00:00Z",
		"step": 10
	}
}
```

### pod_list
##### args type: None

### node_list
##### args type: None

### container_recommendation
##### args type: ListPodRecommendationsRequest
```json
{
	"namespaced_name": {
		"namespace": "ns",
		"name": "name"
	},
	"time_range": {
		"start_time": "1970-01-01T00:00:00Z",
		"end_time": "1970-01-01T00:00:00Z",
		"step": 10
	}
}
```


# Method write_data(data_type, args)

### container_prediction
##### args type: CreatePodPredictionsRequest
```json
{
	"pod_predictions": [
		{
			"namespaced_name": {
				"namespace": "ns",
				"name": "name"
			},
			"container_predictions": [
				{
					"name": "test2",
					"predicted_raw_data": [
						{
							"metric_type": "CPU_USAGE_SECONDS_PERCENTAGE",
							"data": [
								{
									"time": "1970-01-01T00:00:00Z",
									"num_value": "122"
								}
							]
						}
					]
				}
			]
		}
	]
}
```

### node_prediction
##### args type: CreatePodPredictionsRequest
```json
{
	"node_predictions": [
		{
			"name": "test_node",
			"predicted_raw_data": {
				"metric_type": "NODE_CPU_USAGE_SECONDS_PERCENTAGE",
				"data": [{
					"time": "1970-01-01T00:00:00Z",
					"num_value": "1024"
				}]
			}
		}
	]
}
```

### container_recommendation
##### args type: CreatePodRecommendationsRequest
** "assign_pod_policy": one of the following types **
* node_priority
```json
{"nodes": ["node1", "node2"]}
```
* node_selector
```json
{
	"selector": {
		"key1": "value1", 
		"key2": "value2"
	}
}
```
* node_name
type: string 

```json
{
	"pod_recommendations": [
		{
			"namespaced_name": {
				"namespace": "ns",
				"name": "name"
			},
			"apply_recommendation_now": true,
			"assign_pod_policy": {
				"time": "1970-01-01T00:00:00Z",
				"node_name": "node1"
			},
			"container_recommendations": [
			{
				"name": "test",
				"limit_recommendations": [
					{
						"metric_type": "NODE_CPU_USAGE_SECONDS_PERCENTAGE",
						"data": [{
							"time": "1970-01-01T00:00:00Z",
							"num_value": "1024"
						}]
					}
				],
				"request_recommendations": [
					{
						"metric_type": "NODE_CPU_USAGE_SECONDS_PERCENTAGE",
						"data": [{
							"time": "1970-01-01T00:00:00Z",
							"num_value": "1024"
						}]
					}
				],
				"initial_limit_recommendations": [
					{
						"metric_type": "NODE_CPU_USAGE_SECONDS_PERCENTAGE",
						"data": [{
							"time": "1970-01-01T00:00:00Z",
							"num_value": "1024"
						}]
					}
				],
				"initial_request_recommendations": [
					{
						"metric_type": "NODE_CPU_USAGE_SECONDS_PERCENTAGE",
						"data": [{
							"time": "1970-01-01T00:00:00Z",
							"num_value": "1024"
						}]
					}
				]
			}]
					
		}
		
	]
}
```