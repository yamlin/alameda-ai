'''gRPC server handler.'''
# pylint: disable=no-member,invalid-name,import-error

from google.protobuf import struct_pb2

import status
from alameda_api.v1alpha1.ai_service import ai_service_pb2, ai_service_pb2_grpc


# create a class to define the server functions
class AlamedaServicer(ai_service_pb2_grpc.AlamendaAIServiceServicer):
    '''gRPC server handler.'''

    def __init__(self, dao):
        self.dao = dao

    @classmethod
    def _type_value_to_name(cls, val):
        ''' Map the enumeration to a value string. '''
        desc = ai_service_pb2.Object.Type.DESCRIPTOR
        for (k, v) in desc.values_by_name.items():
            if v.number == val:
                return k
        return None # if val isn't a value in MyEnumType

    @classmethod
    def _policy_value_to_name(cls, val):
        ''' Map the enumeration to a value string. '''
        desc = ai_service_pb2.RecommendationPolicy.DESCRIPTOR
        for (k, v) in desc.values_by_name.items():
            if v.number == val:
                return k
        return None # if val isn't a value in MyEnumType

    def _map_object(self, obj):
        ''' Map the object to dictionary '''
        return {
            "namespace": obj.namespace,
            "uid": obj.uid,
            "name": obj.name,
            "type": self._type_value_to_name(obj.type),
            "policy": self._policy_value_to_name(obj.policy)
        }

    def CreatePredictionObjects(self, request, context):
        ''' Endpoint for pod creation.
        Arguments:
            request: request object.
            context: the request context.
        Return:
            RequestResponse
        '''
        with status.context(context):
            pods = map(self._map_object, request.objects)
            self.dao.create_pod(pods)
            return ai_service_pb2.RequestResponse(message="ok")

    def DeletePredictionObjects(self, request, context):
        ''' Endpoint for pod deletion.
        Arguments:
            request: request object.
            context: the request context.
        Return:
            Empty Object
        '''
        with status.context(context):
            pods = map(self._map_object, request.objects)
            self.dao.delete_pod(pods)
            return struct_pb2.Value()
