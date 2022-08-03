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

from datetime import datetime
import logging
from typing import Optional

from th2_common_utils import create_event, create_event_id, message_to_table
from th2_grpc_common.common_pb2 import Event, EventID, EventStatus, Message

logger = logging.getLogger()


def create_grpc_method_event(request_event_id: EventID,
                             method_name: str,
                             description: Optional[str]) -> Event:
    if not description:
        description = f'{method_name} - {datetime.now()}'

    event: Event = create_event(event_id=create_event_id(),
                                parent_id=request_event_id,
                                name=description,
                                event_type=method_name)
    return event


def create_responses_root_event(method_event_id: EventID,
                                status: int,
                                event_name: str) -> Event:
    event: Event = create_event(event_id=create_event_id(),
                                parent_id=method_event_id,
                                status=status,
                                name=event_name,
                                event_type='Incoming messages')
    return event


def create_send_request_succeed_event(message: Message, act_event_id: EventID) -> Event:
    event: Event = create_event(event_id=create_event_id(),
                                parent_id=act_event_id,
                                name=f"Send '{message.metadata.message_type}' message from Act",
                                event_type='Outgoing message',
                                body=_convert_message_to_event_body(message))
    return event


def create_receive_response_succeed_event(response: Message,
                                          responses_root_event: EventID,
                                          status: int) -> Event:
    event: Event = create_event(event_id=create_event_id(),
                                parent_id=responses_root_event,
                                event_type='Incoming message',
                                status=status,
                                name=response.metadata.message_type,
                                body=_convert_message_to_event_body(response),
                                attached_message_ids=[response.metadata.id])
    return event


def create_receive_response_failed_event(text: str, responses_root_event: EventID) -> Event:
    event: Event = create_event(event_id=create_event_id(),
                                parent_id=responses_root_event,
                                event_type='Act message',
                                status=EventStatus.FAILED,
                                name=text)
    return event


def create_context_cancelled_event(method_event_id: EventID) -> Event:
    event: Event = create_event(event_id=create_event_id(),
                                parent_id=method_event_id,
                                event_type='gRPC context failure',
                                status=EventStatus.FAILED,
                                name='gRPC context was cancelled by client')
    return event


def _convert_message_to_event_body(message: Message) -> bytes:
    if isinstance(message, Message):
        message_event_body = message_to_table(message)
        return bytes(message_event_body)

    return b''
