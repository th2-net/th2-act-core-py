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
from typing import Optional

from th2_common_utils import create_event, create_event_id, message_to_table
from th2_grpc_common.common_pb2 import Event, EventID, EventStatus, Message


def create_act_event(request_event_id: EventID,
                     act_name: str,
                     description: Optional[str]) -> Event:
    if not description:
        description = f'{act_name} - {datetime.now()}'

    event: Event = create_event(event_id=create_event_id(),
                                parent_id=request_event_id,
                                name=description,
                                event_type=act_name)

    return event


def create_received_messages_root_event(act_event_id: EventID,
                                        status: int,
                                        event_name: str) -> Event:
    event: Event = create_event(event_id=create_event_id(),
                                parent_id=act_event_id,
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


def create_send_request_failed_event(message: Message, act_event_id: EventID) -> Event:
    event: Event = create_event(event_id=create_event_id(),
                                parent_id=act_event_id,
                                status=EventStatus.FAILED,
                                name=f"Cannot send '{message.metadata.message_type}' message",
                                event_type='Outgoing message',
                                body=_convert_message_to_event_body(message))

    return event


def create_receive_message_succeed_event(received_message: Message,
                                         root_event_id: EventID,
                                         status: int) -> Event:
    event: Event = create_event(event_id=create_event_id(),
                                parent_id=root_event_id,
                                event_type='Incoming message',
                                status=status,
                                name=received_message.metadata.message_type,
                                body=_convert_message_to_event_body(received_message),
                                attached_message_ids=[received_message.metadata.id])

    return event


def create_receive_message_failed_event(text: str, root_event_id: EventID) -> Event:
    event: Event = create_event(event_id=create_event_id(),
                                parent_id=root_event_id,
                                event_type='Act message',
                                status=EventStatus.FAILED,
                                name=text)

    return event


def _convert_message_to_event_body(message: Message) -> bytes:
    message_event_body = message_to_table(message)

    return bytes(message_event_body)
