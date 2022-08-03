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
from typing import Dict, Iterable, Iterator, List, Union

from th2_grpc_common.common_pb2 import Message

logger = logging.getLogger()


StatusMessagesDict = Dict[int, List[Message]]


class Cache:
    """Stores a cache of received messages."""

    def __init__(self) -> None:
        self.cache: List[Message] = []

    def __iter__(self) -> Iterator:
        return self.cache.__iter__()

    def __getitem__(self, item: Union[int, slice]) -> Union[Message, List[Message]]:
        return self.cache[item]

    @property
    def size(self) -> int:
        return len(self.cache)

    def add(self, messages: Iterable[Message]) -> None:
        self.cache.extend(messages)

    def clear(self) -> None:
        self.cache.clear()
