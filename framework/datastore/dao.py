# pylint: disable=invalid-name,missing-docstring

from google.protobuf import empty_pb2
from google.protobuf.json_format import MessageToDict, ParseDict
import grpc
from alameda_api.v1alpha1.datahub import server_pb2, server_pb2_grpc
from framework.log.logger import Logger
from framework.utils.sys_utils import get_datahub_server


class DatahubClient(object):
    def __init__(self, config=None, client=None):
        ''' The construct method '''
        self.client = client
        if not config:
            config = {
                "server": get_datahub_server()
            }
        self.config = config
        self.logger = Logger()
        self.logger.info("DAO config: %s", str(self.config))

    def __get_client(self):
        ''' Get the grpc client '''
        if self.client:
            return self.client

        conn_str = self.config["server"]
        channel = grpc.insecure_channel(conn_str)
        return server_pb2_grpc.DatahubServiceStub(channel)

    def get_data(self, data_type, args):
        client = self.__get_client()
        if data_type == "container_init":
            pb = ParseDict(args, server_pb2.ListPodMetricsRequest())
            res = MessageToDict(client.ListPodMetrics(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        elif data_type == "container_observed":
            pb = ParseDict(args, server_pb2.ListPodMetricsRequest())
            res = MessageToDict(client.ListPodMetrics(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        elif data_type == "container_predicted":
            pb = ParseDict(args, server_pb2.ListPodPredictionsRequest())
            res = MessageToDict(client.ListPodPredictions(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        elif data_type == "node_predicted":
            pb = ParseDict(args, server_pb2.ListNodePredictionsRequest())
            res = MessageToDict(client.ListNodePredictions(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        elif data_type == "node_observed":
            pb = ParseDict(args, server_pb2.ListNodeMetricsRequest())
            res = MessageToDict(client.ListNodeMetrics(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        elif data_type == "pod_list":
            # provide the empty dictionary
            pb = empty_pb2.Empty()
            res = MessageToDict(client.ListAlamedaPods(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        elif data_type == "node_list":
            pb = empty_pb2.Empty()
            res = MessageToDict(client.ListAlamedaNodes(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        elif data_type == "container_init_resource":
            pb = ParseDict(args, server_pb2.ListPodPredictionsRequest())
            res = MessageToDict(client.ListPodPredictions(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        elif data_type == "container_resource":
            pb = ParseDict(args, server_pb2.ListPodPredictionsRequest())
            res = MessageToDict(client.ListPodPredictions(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        else:
            raise Exception("Invalid data type")

        if res["status"]["code"] != 0:
            msg = "Error: data_type {} request is fail: code={}, msg={}".format(
                data_type, res.status.code, res.status.message
            )
            self.logger.error(msg)
            raise Exception(msg)
        return res

    def write_data(self, data_type, args):
        self.logger.info("Write data: type = %s args = %s", data_type, str(args))
        client = self.__get_client()
        if data_type == "container_prediction":
            pb = ParseDict(args, server_pb2.CreatePodPredictionsRequest())
            res = MessageToDict(client.CreatePodPredictions(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        elif data_type == "node_prediction":
            pb = ParseDict(args, server_pb2.CreateNodePredictionsRequest())
            res = MessageToDict(client.CreateNodePredictions(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        elif data_type == "container_recommendation":
            pb = ParseDict(args, server_pb2.CreatePodRecommendationsRequest())
            res = MessageToDict(client.CreatePodRecommendations(pb),
                                preserving_proto_field_name=True,
                                including_default_value_fields=True)
        else:
            raise Exception("Invalid data type")

        if res["status"]["code"] != 0:
            msg = "Error: data_type {} request is fail: code={}, msg={}".format(
                data_type, res.status.code, res.status.message
            )
            self.logger.error(msg)
            raise Exception(msg)

        return res
