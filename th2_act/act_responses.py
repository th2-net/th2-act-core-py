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
from typing import List, Optional, Union

from google.protobuf.text_format import MessageToString
from th2_act.util.abstract_act_responses import AbstractActMultiResponse, AbstractActResponsesList, \
    AbstractActSingleResponse
import th2_act.util.act_events as events
from th2_grpc_common.common_pb2 import Checkpoint, Event, EventID, Message, RequestStatus

logger = logging.getLogger()


class ActSingleResponse(AbstractActSingleResponse):
    """ActSingleResponse may be returned to act implementation by Act if expected message was received successfully.
    The class instance contain message, status and checkpoint.
    """

    def __init__(self,
                 message: Message,
                 status: Union[str, int],
                 checkpoint: Optional[Checkpoint] = None) -> None:
        super().__init__(message=message,
                         status=status,
                         checkpoint=checkpoint)

    def __len__(self) -> int:
        return 1

    def create_events(self, responses_root_event: EventID, runtime: float) -> List[Event]:
        logger.debug('Filtered response message (in %d s): %s'
                     % (round(runtime, 1), MessageToString(self.message, as_one_line=True)))  # type: ignore

        return [
            events.create_receive_response_succeed_event(response=self.message,  # type: ignore
                                                         status=self.status.status,
                                                         responses_root_event=responses_root_event)
        ]


class ActErrorSingleResponse(AbstractActSingleResponse):
    """ActErrorSingleResponse may be returned to act implementation by Act if expected message was not received.
    The class instance contain message (set to None), status and checkpoint.
    """

    def __init__(self, checkpoint: Checkpoint) -> None:
        super().__init__(message=None,
                         status=RequestStatus.ERROR,
                         checkpoint=checkpoint)

    def __len__(self) -> int:
        return 0

    def create_events(self, responses_root_event: EventID, runtime: float) -> List[Event]:
        error_text = f'Message matching the condition was not found in {round(runtime, 1)} s'
        logger.error(error_text)

        return [
            events.create_receive_response_failed_event(text=error_text,
                                                        responses_root_event=responses_root_event)
        ]


class ActMultiResponse(AbstractActMultiResponse):
    """ActMultiResponse may be returned to act implementation by Act if expected messages were received successfully.
    The class instance contain list of messages, status and checkpoint.
    """

    def __init__(self,
                 messages: List[Message],
                 status: Union[str, int],
                 checkpoint: Checkpoint) -> None:
        super().__init__(messages=messages,
                         status=status,
                         checkpoint=checkpoint)

    def create_events(self, responses_root_event: EventID, runtime: float) -> List[Event]:
        logger.debug('Filtered response messages (in %d s): %s'
                     % (round(runtime, 1), [MessageToString(m, as_one_line=True) for m in self.messages]))

        return [
            events.create_receive_response_succeed_event(response=message, status=self.status.status,
                                                         responses_root_event=responses_root_event)
            for message in self.messages
        ]


class ActErrorMultiResponse(AbstractActMultiResponse):
    """ActErrorMultiResponse may be returned to act implementation by Act if expected messages were not received.
    The class instance contain empty list, status and checkpoint.
    """

    def __init__(self, checkpoint: Checkpoint) -> None:
        super().__init__(messages=[],
                         status=RequestStatus.ERROR,
                         checkpoint=checkpoint)

    def create_events(self, responses_root_event: EventID, runtime: float) -> List[Event]:
        error_text = f'Messages matching the condition were not found in {round(runtime, 1)} s'
        logger.error(error_text)

        return [
            events.create_receive_response_failed_event(text=error_text,
                                                        responses_root_event=responses_root_event)
        ]


class ActResponsesList(AbstractActResponsesList):
    """ActResponsesList may be returned to act implementation by Act if expected messages were received successfully.
    The class instance contain list of ActSingleResponse and checkpoint.
    """

    def __init__(self, act_responses: List[ActSingleResponse], checkpoint: Checkpoint) -> None:
        super().__init__(act_responses=act_responses,  # type: ignore
                         checkpoint=checkpoint)

    def create_events(self, responses_root_event: EventID, runtime: float) -> List[Event]:
        logger.debug('Filtered response messages (in %d s): %s'
                     % (round(runtime, 1),
                        [
                            MessageToString(act_response.message, as_one_line=True)  # type: ignore
                            for act_response in self.act_responses
                        ])
                     )

        return [
            events.create_receive_response_succeed_event(response=response.message,  # type: ignore
                                                         status=response.status.status,
                                                         responses_root_event=responses_root_event)
            for response in self.act_responses
        ]


class ActErrorResponsesList(AbstractActResponsesList):
    """ActErrorResponsesList may be returned to act implementation by Act if expected messages were not received.
    The class instance contain empty list and checkpoint.
    """

    def __init__(self, checkpoint: Checkpoint) -> None:
        super().__init__(act_responses=[],
                         checkpoint=checkpoint)

    def create_events(self, responses_root_event: EventID, runtime: float) -> List[Event]:
        error_text = f'Messages matching the condition were not found in {round(runtime, 1)} s'
        logger.error(error_text)

        return [
            events.create_receive_response_failed_event(text=error_text,
                                                        responses_root_event=responses_root_event)
        ]


class ActResponseFactory:

    def __init__(self, checkpoint: Checkpoint) -> None:
        self.checkpoint = checkpoint

    def get_single_response(self, status_messages_dict: dict) -> AbstractActSingleResponse:
        if len(status_messages_dict) == 1:
            for status, messages in status_messages_dict.items():
                return ActSingleResponse(message=messages[0],
                                         status=status,
                                         checkpoint=self.checkpoint)

        return ActErrorSingleResponse(checkpoint=self.checkpoint)

    def get_multi_response(self, status_messages_dict: dict) -> AbstractActMultiResponse:
        if len(status_messages_dict) == 1:
            for status, messages in status_messages_dict.items():
                return ActMultiResponse(messages=messages,
                                        status=status,
                                        checkpoint=self.checkpoint)

        return ActErrorMultiResponse(checkpoint=self.checkpoint)

    def get_responses_list(self, status_messages_dict: dict) -> AbstractActResponsesList:
        if len(status_messages_dict) > 0:
            return ActResponsesList(
                act_responses=[
                    ActSingleResponse(message=message, status=status)
                    for status in status_messages_dict
                    for message in status_messages_dict[status]
                ],
                checkpoint=self.checkpoint
            )

        return ActErrorResponsesList(checkpoint=self.checkpoint)
