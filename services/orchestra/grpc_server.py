'''gRPC server to receive requests from remote.'''
# pylint: disable=import-error

import tempfile
import time
from concurrent import futures
from argparse import ArgumentParser

import grpc

from framework.log.logger import Logger
from framework.datastore.file_dao import FileDataStore
from framework.filesystem.fsal import FileSystem, FileSystemType
from alameda_api.v1alpha1.ai_service import ai_service_pb2_grpc
from services.orchestra.alameda_servicer import AlamedaServicer

_ONE_DAY_IN_SECONDS = 24 * 60 * 60


def setup_arguments():
    '''Setup arguments.

    Args: None

    Returns:
        args: Argument object.
    '''

    parser = ArgumentParser(prog="Demo Server",
                            description="A simple grpc demo server.")
    parser.add_argument('--file_type')

    return parser.parse_args()

def main():
    '''Main entrypoint for gRPC server.'''

    args = setup_arguments()
    log = Logger()

    if args.file_type == "local":
        file_system = FileSystem(fstype=FileSystemType.FSTYPE_LOCAL)
        file_system.impl.remote_root_dirpath = tempfile.mkdtemp()
        dao = FileDataStore(file_system=file_system)
    else:
        dao = FileDataStore()

    # create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    handler = AlamedaServicer(dao)
    # use the generated function `add_CalculatorServicer_to_server`
    # to add the defined class to the created server
    ai_service_pb2_grpc.add_AlamendaAIServiceServicer_to_server(
        handler, server)

    # listen on port 50051
    log.info('Starting server. Listening on port 50051.')
    server.add_insecure_port('[::]:50051')
    server.start()

    # since server.start() will not block,
    # a sleep-loop is added to keep alive
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    main()
