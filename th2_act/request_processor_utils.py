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
from typing import Dict, List, Optional, Union

from th2_act.act_connector import ActConnector
from th2_act.act_parameters import ActParameters
from th2_act.act_response import ActMultiResponse, ActResponse
from th2_act.cache_processor import CacheProcessor
from th2_act.message_response_monitor import MessageResponseMonitor
from th2_grpc_common.common_pb2 import EventID, Message, RequestStatus

logger = logging.getLogger()


class RequestProcessorUtils:

    def __init__(self,
                 act_conn: ActConnector,
                 act_parameters: ActParameters,
                 cache_processor: CacheProcessor):
        self.act_conn = act_conn
        self.act_name = act_parameters.act_name
        self.context = act_parameters.context
        self.cache_processor = cache_processor

        self.act_event_id = act_conn.store_act_event(request_event_id=act_parameters.request_event_id,
                                                     act_name=act_parameters.act_name,
                                                     description=act_parameters.request_description)
        self.checkpoint = self.act_conn.register_checkpoint(act_event_id=self.act_event_id)

        self.response_monitor = MessageResponseMonitor()

    def send_message(self, message: Message) -> ActResponse:
        if self.context.is_active():
            return self.act_conn.send_message(message=message,
                                              act_event_id=self.act_event_id,
                                              checkpoint=self.checkpoint)

        else:
            warning_text = f'{self.act_name} request cancelled by client'
            logger.warning(warning_text)
            self.act_conn.store_send_request_failed_event(message=message, act_event_id=self.act_event_id)

            return ActResponse(status=RequestStatus.ERROR)

    def wait(self, seconds: Union[int, float]) -> None:
        if self.context.time_remaining() > seconds:
            self.response_monitor.await_sync(seconds)
        else:
            logger.warning('Remaining time of context less than wait time. Cache check starts now.')

    def create_received_messages_root_event(self,
                                            method_name: str,
                                            messages_received: int,
                                            messages_expected: Optional[int] = None) -> EventID:
        if messages_expected in (1, None):
            return self.act_conn.store_received_messages_root_event(act_event_id=self.act_event_id,
                                                                    status=RequestStatus.SUCCESS,
                                                                    event_name=f'Receive {messages_received} message(s)'
                                                                               f' - {method_name}')
        elif messages_received == messages_expected:
            return self.act_conn.store_received_messages_root_event(act_event_id=self.act_event_id,
                                                                    status=RequestStatus.SUCCESS,
                                                                    event_name=f'Receive {messages_received}/'
                                                                               f'{messages_expected} message(s)'
                                                                               f' - {method_name}')
        else:
            return self.act_conn.store_received_messages_root_event(act_event_id=self.act_event_id,
                                                                    status=RequestStatus.ERROR,
                                                                    event_name=f'Receive {messages_received}/'
                                                                               f'{messages_expected} message(s)'
                                                                               f' - {method_name}')

    def create_received_messages_events(self,
                                        status_messages_dict: Dict[int, List[Message]],
                                        root_event_id: EventID,
                                        runtime: float) -> None:
        if status_messages_dict:
            for status, messages in status_messages_dict.items():
                for message in messages:
                    self.act_conn.store_process_received_message_succeed_event(received_message=message,
                                                                               root_event_id=root_event_id,
                                                                               status=status)

        else:
            error_text = f'Message matching the condition was not found in {runtime} s'
            self.act_conn.store_process_received_message_failed_event(text=error_text, root_event_id=root_event_id)

    def create_act_responses_list(self,
                                  status_messages_dict: Dict[int, List[Message]],
                                  root_event_id: EventID) -> List[ActResponse]:
        if not self.context.is_active():
            return [self._request_cancelled_act_response(root_event_id)]

        elif status_messages_dict:
            return [
                ActResponse(message=message, status=status, checkpoint=self.checkpoint)
                for status, messages in status_messages_dict.items()
                for message in messages
            ]

        return [ActResponse(status=RequestStatus.ERROR, checkpoint=self.checkpoint)]

    def create_act_multi_response(self,
                                  status_messages_dict: Dict[int, List[Message]],
                                  root_event_id: EventID) -> ActMultiResponse:
        if not self.context.is_active():
            return self._request_cancelled_act_multi_response(root_event_id)

        elif status_messages_dict:
            for status, messages in status_messages_dict.items():
                return ActMultiResponse(messages=messages,
                                        status=status,
                                        checkpoint=self.checkpoint)

        return ActMultiResponse(status=RequestStatus.ERROR, checkpoint=self.checkpoint)

    def _request_cancelled_act_response(self, root_event_id: EventID) -> ActResponse:
        warning_text = f'{self.act_name} cancelled by client'
        logger.warning(warning_text)
        self.act_conn.store_process_received_message_failed_event(text=warning_text, root_event_id=root_event_id)

        return ActResponse(status=RequestStatus.ERROR)

    def _request_cancelled_act_multi_response(self, root_event_id: EventID) -> ActMultiResponse:
        warning_text = f'{self.act_name} cancelled by client'
        logger.warning(warning_text)
        self.act_conn.store_process_received_message_failed_event(text=warning_text, root_event_id=root_event_id)

        return ActMultiResponse(status=RequestStatus.ERROR)
