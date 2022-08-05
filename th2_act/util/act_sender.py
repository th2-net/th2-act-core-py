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
import traceback

from th2_act.grpc_method_attributes import GrpcMethodAttributes
import th2_act.util.act_events as events
from th2_act.util.grpc_context_manager import GRPCContextManager
from th2_common.schema.message.message_router import MessageRouter
from th2_grpc_check1.check1_pb2 import CheckpointRequest, CheckpointResponse
from th2_grpc_check1.check1_service import Check1Service
from th2_grpc_common.common_pb2 import Checkpoint, Event, EventBatch, EventID, Message, MessageBatch

logger = logging.getLogger()


class ActSender:
    """Sends messages and events."""

    def __init__(self,
                 message_router: MessageRouter,
                 event_router: MessageRouter,
                 check1_connector: Check1Service,
                 grpc_context_manager: GRPCContextManager,
                 method_attrs: GrpcMethodAttributes):
        self.message_router = message_router
        self.event_router = event_router
        self.check1_connector = check1_connector

        self.grpc_context_manager = grpc_context_manager
        self.method_name = method_attrs.method_name

        self.is_sending_allowed = True

        self.method_event_id = self.create_and_send_method_event(request_event_id=method_attrs.request_event_id,
                                                                 method_name=method_attrs.method_name,
                                                                 description=method_attrs.request_description)
        self.checkpoint = self.register_checkpoint()

    def register_checkpoint(self) -> Checkpoint:
        checkpoint = CheckpointRequest(parent_event_id=self.method_event_id)
        check1_response: CheckpointResponse = self.check1_connector.createCheckpoint(request=checkpoint)

        return check1_response.checkpoint

    def send_message(self, message: Message) -> bool:
        message.parent_event_id.CopyFrom(self.method_event_id)

        if self._send_with_message_router(message):
            event = events.create_send_request_succeed_event(message=message, act_event_id=self.method_event_id)
            self._send_with_event_router(event)

            logger.debug('Message was sent successfully: %s' % message)
            return True

        return False

    def create_and_send_method_event(self, request_event_id: EventID, method_name: str, description: str) -> EventID:
        event = events.create_grpc_method_event(request_event_id=request_event_id,
                                                method_name=method_name,
                                                description=description)
        self._send_with_event_router(event)

        return event.id

    def create_and_send_responses_root_event(self, status: int, event_name: str) -> EventID:
        event = events.create_responses_root_event(method_event_id=self.method_event_id,
                                                   status=status,
                                                   event_name=event_name)
        self._send_with_event_router(event)
        return event.id

    def create_and_send_response_succeed_event(self,
                                               response: Message,
                                               responses_root_event: EventID,
                                               status: int) -> EventID:
        event = events.create_receive_response_succeed_event(response=response,
                                                             responses_root_event=responses_root_event,
                                                             status=status)
        self._send_with_event_router(event)

        return event.id

    def create_and_send_response_failed_event(self, text: str, responses_root_event: EventID) -> EventID:
        event = events.create_receive_response_failed_event(text=text,
                                                            responses_root_event=responses_root_event)
        self._send_with_event_router(event)

        return event.id

    def _send_with_event_router(self, event: Event) -> None:
        try:
            if not self.is_sending_allowed:
                return
            elif self.grpc_context_manager.is_context_active():
                self.event_router.send_all(message=EventBatch(events=[event]))
            else:
                self.is_sending_allowed = False
                event = events.create_context_cancelled_event(self.method_event_id)
                self.event_router.send_all(message=EventBatch(events=[event]))
                logger.warning(f'Cannot send event: {self.method_name} gRPC context was cancelled by client')

        except Exception as e:
            logger.error(f'Cannot send event "{event.name}" with attached messages IDs '
                         f'{event.attached_message_ids} to estore: {e}'
                         f'\n{"".join(traceback.format_tb(e.__traceback__))}')

    def _send_with_message_router(self, message: Message) -> bool:
        try:
            if not self.is_sending_allowed:
                return False

            elif self.grpc_context_manager.is_context_active():
                self.message_router.send(MessageBatch(messages=[message]))
                return True

            else:
                self.is_sending_allowed = False
                event = events.create_context_cancelled_event(self.method_event_id)
                self.event_router.send_all(message=EventBatch(events=[event]))
                logger.warning(f'Cannot send message: {self.method_name} gRPC context was cancelled by client')
                return False

        except Exception as e:
            logger.error(f'Cannot send the message with session alias {e}'
                         f'{message.metadata.id.connection_id.session_alias}: '
                         f'\n{"".join(traceback.format_tb(e.__traceback__))}')
            return False
