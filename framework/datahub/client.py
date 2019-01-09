'''Client to connect to datahub via gRPC.'''

import os
import yaml
import grpc
from google.protobuf import empty_pb2
from google.protobuf.json_format import MessageToDict, ParseDict
from alameda_api.v1alpha1.datahub import server_pb2, server_pb2_grpc
from framework.log.logger import Logger
from framework.utils.sys_utils import get_datahub_server


class DatahubClient(object):
    '''Client to connect to datahub via gRPC.'''

    METHOD_GET = 'get_methods'
    METHOD_PUT = 'put_methods'

    def __init__(self, client=None, config=None):
        self.client = client
        self.config = config
        self.logger = Logger()
        self.grpc_mapping = None

    def __get_client(self):
        '''Get the gRPC client.

        There two methods to establish a client:
        (1) Use external client (primary)
        (2) Use configuration
        '''

        if self.client:
            return self.client

        if not self.config:
            self.config = {
                "server": get_datahub_server()
            }

        self.logger.info("DAO config: %s", str(self.config))

        conn_str = self.config["server"]
        channel = grpc.insecure_channel(conn_str)
        self.client = server_pb2_grpc.DatahubServiceStub(channel)

        return self.client

    def __get_grpc_mapping(self):
        '''Get gRPC mapping rules.'''

        if self.grpc_mapping:
            return self.grpc_mapping

        curdir = os.path.dirname(os.path.realpath(__file__))
        config = open(os.path.join(curdir, "grpc_mapping.yaml"), "r")
        self.grpc_mapping = yaml.load(config)

        return self.grpc_mapping

    def __check_response(self, data_type, response):
        '''Check the gRPC reponse.'''

        status = None

        if "status" in response:
            status = response["status"]
        else:
            status = response

        if status["code"] == 0:
            return response

        fmt = "Error: data_type {} request is fail: code={}, msg={}"
        msg = fmt.format(data_type,
                         response['status']['code'],
                         response['status']['message'])
        self.logger.error(msg)
        raise Exception(msg)

    def __transmit(self, data_type, method, args):
        '''Transmit data to gRPC server and get a response.'''

        client = self.__get_client()
        mapping = self.__get_grpc_mapping()

        rules = mapping[method][data_type]
        if 'request' not in rules or 'function' not in rules:
            raise Exception("Unknwon data type '%s'" % data_type)

        function = getattr(client, rules['function'])
        request = getattr(server_pb2, rules['request']) \
            if rules['request'] is not None else empty_pb2.Empty
        method = rules['method']
        params = rules['parameters'] if 'parameters' in rules else {}

        message = ParseDict(args, request())
        res = MessageToDict(function(message), **params)

        return self.__check_response(data_type, res)

    def get_data(self, data_type, args):
        '''Get data from gRPC server.

        Args:
            data_type(str): Data type to access gRPC.
            args(dict): Arguments in a dictionary format.

        Returns:
            response(dict): A response from gRPC server.
        '''

        return self.__transmit(data_type, self.METHOD_GET, args)

    def write_data(self, data_type, args):
        '''Write data to gRPC server.

        Args:
            data_type(str): Data type to access gRPC.
            args(dict): Arguments in a dictionary format.

        Returns:
            response(dict): A response from gRPC server.
        '''

        return self.__transmit(data_type, self.METHOD_PUT, args)
