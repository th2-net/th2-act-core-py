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
from typing import Iterator, List, Optional, Union

from th2_grpc_common.common_pb2 import Checkpoint, Event, EventID, Message, RequestStatus

logger = logging.getLogger()


class AbstractActResponse:

    __slots__ = 'checkpoint'

    def __init__(self, checkpoint: Optional[Checkpoint]) -> None:
        self.checkpoint = checkpoint

    def __len__(self) -> int:
        pass

    def create_events(self, responses_root_event: EventID, runtime: float) -> List[Event]:
        pass


class AbstractActSingleResponse(AbstractActResponse):

    __slots__ = ('message', 'status')

    def __init__(self,
                 message: Optional[Message],
                 status: Union[str, int],
                 checkpoint: Optional[Checkpoint]) -> None:
        self.message = message
        self.status = RequestStatus(status=status)  # type: ignore
        super().__init__(checkpoint=checkpoint)

    def __len__(self) -> int:
        pass

    def create_events(self, responses_root_event: EventID, runtime: float) -> List[Event]:
        pass


class AbstractActMultiResponse(AbstractActResponse):

    __slots__ = ('messages', 'status')

    def __init__(self,
                 messages: List[Message],
                 status: Union[str, int],
                 checkpoint: Checkpoint) -> None:
        self.messages = messages
        self.status = RequestStatus(status=status)  # type: ignore
        super().__init__(checkpoint=checkpoint)

    def __iter__(self) -> Iterator:
        return self.messages.__iter__()

    def __getitem__(self, item: int) -> Message:
        return self.messages[item]

    def __len__(self) -> int:
        return len(self.messages)

    def create_events(self, responses_root_event: EventID, runtime: float) -> List[Event]:
        pass


class AbstractActResponsesList(AbstractActResponse):

    __slots__ = 'act_responses'

    def __init__(self, act_responses: List[AbstractActSingleResponse], checkpoint: Checkpoint) -> None:
        self.act_responses = act_responses
        super().__init__(checkpoint=checkpoint)

    def __iter__(self) -> Iterator:
        return self.act_responses.__iter__()

    def __getitem__(self, item: int) -> AbstractActSingleResponse:
        return self.act_responses[item]

    def __len__(self) -> int:
        return len(self.act_responses)

    def create_events(self, responses_root_event: EventID, runtime: float) -> List[Event]:
        pass
