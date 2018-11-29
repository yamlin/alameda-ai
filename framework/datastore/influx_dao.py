from influxdb import InfluxDBClient

class InfluxDataStore(object):
    _DISK_TABLE = 'prediction_disk'
    _POD_TABLE = 'prediction_pod'

    def __init__(self, client=None):
        self.client = client

    def _query_data(self, query, database=None):
        return self.client.query(query, database=database)

    def create_disk(self, data):
        points = [
            {
                "measurement": self._DISK_TABLE,
                "tags": {
                    "disk_id": data['id'],
                },
                "fields": {
                    "name": data['name'],
                }
            }
        ]
        return self.client.write_points(points=points)

    def delete_disk(self, id):
        params = {
            'disk_id': id,
            'measurement': self._DISK_TABLE
        }
        query = "DELETE FROM {measurement} " \
                " WHERE disk_id='{disk_id}'" \
                .format(**params)
        return self._query_data(query)
    
    def create_pod(self, data):
        points = [
            {
                "measurement": self._POD_TABLE,
                "tags": {
                    "pod_id": data['id'],
                },
                "fields": {
                    "name": data['name'],
                }
            }
        ]
        return self.client.write_points(
            points=points
        )
    
    def delete_pod(self, id):
        params = {
            "pod_id": id,
            "measurement": self._POD_TABLE
        }
        query = "DELETE FROM {measurement} " \
                " WHERE pod_id='{pod_id}'" \
                .format(**params)
        return self._query_data(query)
