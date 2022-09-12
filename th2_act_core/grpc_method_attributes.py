#   Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import grpc
from th2_grpc_common.common_pb2 import EventID


class GrpcMethodAttributes:
    """Contains parameters of gRPC implementation method. GrpcMethodAttributes are used in RequestProcessor
    context manager.

    Args:
        context (grpc.RpcContext): gRPC method context.
        request_event_id (EventID): Request event ID.
        method_name (str): Act name. This will be shown in GIU as gRPC method event name.
        request_description (str, optional): Request description. This will be shown in GIU as request event name.
    """

    __slots__ = ('context', 'method_name', 'request_event_id', 'request_description')

    def __init__(self,
                 context: grpc.RpcContext,
                 method_name: str,
                 request_event_id: EventID,
                 request_description: str = ''):
        self.context = context
        self.method_name = method_name
        self.request_event_id = request_event_id
        self.request_description = request_description
