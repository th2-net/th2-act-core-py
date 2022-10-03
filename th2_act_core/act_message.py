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

from dataclasses import dataclass
import logging
from typing import List, Optional, Union

from google.protobuf.text_format import MessageToString
import th2_act_core.util.act_events as events
from th2_grpc_common.common_pb2 import Event, EventID, Message, RequestStatus

logger = logging.getLogger()


@dataclass
class ActMessage:
    """ActMessage is returned by RequestProcessor's 'search' methods. The class instance can contain message
    (if any message received, None otherwise) and status (RequestStatus.SUCCESS or RequestStatus.ERROR).
    """

    message: Optional[Message] = None
    status: Union[str, int] = RequestStatus.ERROR


class ActMessageUtil:

    @staticmethod
    def get_length(act_messages: Union[ActMessage, List[ActMessage]]) -> int:
        if isinstance(act_messages, list):
            return len(act_messages)

        elif act_messages.message is not None:
            return 1

        return 0

    @staticmethod
    def create_events(act_messages: Union[ActMessage, List[ActMessage]],
                      runtime: float,
                      responses_root_event: EventID) -> List[Event]:
        if isinstance(act_messages, list):
            if len(act_messages) > 0:
                return ActMessageUtil._create_succeed_event_list(act_messages=act_messages,
                                                                 runtime=runtime,
                                                                 responses_root_event=responses_root_event)
            else:
                return ActMessageUtil._create_failed_event_list(runtime=runtime,
                                                                responses_root_event=responses_root_event)
        elif isinstance(act_messages, ActMessage):
            if act_messages.message is not None:
                return ActMessageUtil._create_succeed_event(act_message=act_messages,
                                                            responses_root_event=responses_root_event,
                                                            runtime=runtime)
            else:
                return ActMessageUtil._create_failed_event(responses_root_event=responses_root_event,
                                                           runtime=runtime)

        return []

    @staticmethod
    def _create_succeed_event_list(act_messages: List[ActMessage],
                                   runtime: float,
                                   responses_root_event: EventID) -> List[Event]:
        logger.debug('Filtered response messages (in %d s): %s'
                     % (round(runtime, 1),
                        [
                            MessageToString(act_response.message, as_one_line=True)  # type: ignore
                            for act_response in act_messages
                        ])
                     )
        return [
            events.create_receive_response_succeed_event(response=act_message.message,  # type: ignore
                                                         status=act_message.status,
                                                         responses_root_event=responses_root_event)
            for act_message in act_messages
        ]

    @staticmethod
    def _create_failed_event_list(runtime: float,
                                  responses_root_event: EventID) -> List[Event]:
        error_text = f'Messages matching the condition were not found in {round(runtime, 1)} s'
        logger.error(error_text)

        return [
            events.create_receive_response_failed_event(text=error_text,
                                                        responses_root_event=responses_root_event)
        ]

    @staticmethod
    def _create_succeed_event(act_message: ActMessage,
                              responses_root_event: EventID, runtime: float) -> List[Event]:
        logger.debug('Filtered response message (in %d s): %s'
                     % (round(runtime, 1), MessageToString(act_message.message, as_one_line=True)))  # type: ignore

        return [
            events.create_receive_response_succeed_event(response=act_message.message,  # type: ignore
                                                         status=act_message.status,
                                                         responses_root_event=responses_root_event)
        ]

    @staticmethod
    def _create_failed_event(responses_root_event: EventID, runtime: float) -> List[Event]:
        error_text = f'Message matching the condition was not found in {round(runtime, 1)} s'
        logger.error(error_text)

        return [
            events.create_receive_response_failed_event(text=error_text,
                                                        responses_root_event=responses_root_event)
        ]
