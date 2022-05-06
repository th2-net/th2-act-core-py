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

from typing import Any

from th2_grpc_common.common_pb2 import EventID


class ActParameters:
    """Contains parameters used in RequestProcessor context manager.

    Args:
        context: gRPC method context.
        request_event_id (EventID): Request event ID.
        act_name (str): Act name. This will be shown in GIU as gRPC method event name.
        request_description (str, optional): Request description. This will be shown in GIU as request event name.
    """

    def __init__(self,
                 context: Any,
                 request_event_id: EventID,
                 act_name: str,
                 request_description: str = ''):
        self.context = context
        self.request_event_id = request_event_id
        self.act_name = act_name
        self.request_description = request_description
