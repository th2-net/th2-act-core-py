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

import logging

import th2_act.act_events as events
from th2_act.act_response import ActResponse
from th2_act.subscription_manager import SubscriptionManager
from th2_common.schema.message.message_router import MessageRouter
from th2_grpc_check1.check1_pb2 import CheckpointRequest, CheckpointResponse
from th2_grpc_check1.check1_service import Check1Service
from th2_grpc_common.common_pb2 import Checkpoint, Event, EventBatch, EventID, Message, MessageBatch, RequestStatus


logger = logging.getLogger()


class ActConnector:
    """Contains methods to send messages and events."""

    def __init__(self,
                 check1_connector: Check1Service,
                 message_router: MessageRouter,
                 event_router: MessageRouter,
                 subscription_manager: SubscriptionManager):
        self.check1_connector = check1_connector
        self.message_router = message_router
        self.event_router = event_router
        self.subscription_manager = subscription_manager

    def store_act_event(self, request_event_id: EventID, act_name: str, description: str) -> EventID:
        event = events.create_act_event(request_event_id=request_event_id,
                                        act_name=act_name,
                                        description=description)
        self._create_and_send_event_batch(event)

        return event.id

    def store_received_messages_root_event(self, act_event_id: EventID, status: int, event_name: str) -> EventID:
        event = events.create_received_messages_root_event(act_event_id=act_event_id,
                                                           status=status,
                                                           event_name=event_name)
        self._create_and_send_event_batch(event)

        return event.id

    def register_checkpoint(self, act_event_id: EventID) -> Checkpoint:
        checkpoint = CheckpointRequest(parent_event_id=act_event_id)
        check1_response: CheckpointResponse = self.check1_connector.createCheckpoint(request=checkpoint)

        return check1_response.checkpoint

    def send_message(self, message: Message, act_event_id: EventID, checkpoint: Checkpoint) -> ActResponse:
        try:
            message.parent_event_id.CopyFrom(act_event_id)

            message_batch = MessageBatch(messages=[message])
            self.message_router.send(message_batch)

            self.store_send_request_succeed_event(message=message, act_event_id=act_event_id)

            return ActResponse(status=RequestStatus.SUCCESS,
                               checkpoint=checkpoint)

        except Exception as e:
            logger.error('Could not send the message with session alias %s: %s'
                         % (message.metadata.id.connection_id.session_alias, e))
            self.store_send_request_failed_event(message=message, act_event_id=act_event_id)

            return ActResponse(status=RequestStatus.ERROR)

    def store_send_request_succeed_event(self, message: Message, act_event_id: EventID) -> EventID:
        event = events.create_send_request_succeed_event(message=message, act_event_id=act_event_id)
        self._create_and_send_event_batch(event)

        return event.id

    def store_send_request_failed_event(self, message: Message, act_event_id: EventID) -> EventID:
        event = events.create_send_request_failed_event(message=message, act_event_id=act_event_id)
        self._create_and_send_event_batch(event)

        return event.id

    def store_process_received_message_succeed_event(self,
                                                     received_message: Message,
                                                     root_event_id: EventID,
                                                     status: int) -> EventID:
        event = events.create_receive_message_succeed_event(received_message=received_message,
                                                            root_event_id=root_event_id,
                                                            status=status)
        self._create_and_send_event_batch(event)

        return event.id

    def store_process_received_message_failed_event(self, text: str, root_event_id: EventID) -> EventID:
        event = events.create_receive_message_failed_event(text, root_event_id)
        self._create_and_send_event_batch(event)

        return event.id

    def _create_and_send_event_batch(self, event: Event) -> None:
        try:
            event_batch = EventBatch(events=[event])
            self.event_router.send_all(message=event_batch)
        except Exception as e:
            logger.error('Could not send event to estore: %s' % e)
