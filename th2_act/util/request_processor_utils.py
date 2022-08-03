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

from th2_act.act_response import ActMultiResponse, ActResponse
from th2_act.util.act_sender import ActSender
from th2_grpc_common.common_pb2 import EventID, Message, RequestStatus

logger = logging.getLogger()


class RequestProcessorUtils:

    ResponsesType = Union[List[ActResponse], ActMultiResponse]

    def __init__(self,
                 act_sender: ActSender):
        self.act_sender = act_sender
        self.checkpoint = act_sender.checkpoint

    def _create_and_send_responses_root_event(self,
                                              receive_method_name: str,
                                              messages_received: int = 0,
                                              messages_expected: Optional[int] = None) -> EventID:
        if messages_expected in (1, None):
            return self.act_sender.create_and_send_responses_root_event(status=RequestStatus.SUCCESS,
                                                                        event_name=f'Received {messages_received} '
                                                                                   f'message(s) - '
                                                                                   f'{receive_method_name}')
        elif messages_received == messages_expected:
            return self.act_sender.create_and_send_responses_root_event(status=RequestStatus.SUCCESS,
                                                                        event_name=f'Received {messages_received}/'
                                                                                   f'{messages_expected} message(s)'
                                                                                   f' - {receive_method_name}')
        else:
            return self.act_sender.create_and_send_responses_root_event(status=RequestStatus.ERROR,
                                                                        event_name=f'Received {messages_received}/'
                                                                                   f'{messages_expected} message(s)'
                                                                                   f' - {receive_method_name}')

    def create_and_send_responses_events(self,
                                         receive_method_name: str,
                                         runtime: float,
                                         act_responses: Optional[ResponsesType],
                                         number_of_responses_expected: Optional[int] = None) -> None:
        if act_responses is None:
            responses_root_event = self._create_and_send_responses_root_event(
                receive_method_name=receive_method_name,
                messages_expected=number_of_responses_expected
            )
            error_text = f'Message matching the condition was not found in {round(runtime, 1)} s'
            self.act_sender.create_and_send_response_failed_event(text=error_text,
                                                                  responses_root_event=responses_root_event)

        else:
            responses_root_event = self._create_and_send_responses_root_event(
                receive_method_name=receive_method_name,
                messages_received=len(act_responses),
                messages_expected=number_of_responses_expected
            )

            if isinstance(act_responses, list):
                for response in act_responses:
                    self.act_sender.create_and_send_response_succeed_event(response=response.message,  # type: ignore
                                                                           responses_root_event=responses_root_event,
                                                                           status=response.status.status)

                logger.debug(f'Received {len(act_responses)} messages: {act_responses}')

            elif isinstance(act_responses, ActMultiResponse):
                for message in act_responses.messages:
                    self.act_sender.create_and_send_response_succeed_event(response=message,
                                                                           responses_root_event=responses_root_event,
                                                                           status=act_responses.status.status)

                logger.debug(f'Received {len(act_responses)} messages: {act_responses.messages}')

    def create_act_responses_list(self,
                                  status_messages_dict: Dict[int, List[Message]]) -> Optional[List[ActResponse]]:
        if status_messages_dict:
            return [
                ActResponse(message=message, status=status, checkpoint=self.checkpoint)
                for status, messages in status_messages_dict.items()
                for message in messages
            ]

        return None

    def create_act_multi_response(self,
                                  status_messages_dict: Dict[int, List[Message]]) -> Optional[ActMultiResponse]:
        for status, messages in status_messages_dict.items():
            return ActMultiResponse(messages=messages,
                                    status=status,
                                    checkpoint=self.checkpoint)

        return None
