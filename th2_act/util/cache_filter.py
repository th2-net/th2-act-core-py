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

from collections import defaultdict
from itertools import islice
import logging
from time import time
from typing import Callable, Dict, Iterator, List, Optional, Tuple, Union

from th2_act.util.act_receiver import check
from th2_act.util.cache import Cache
from th2_act.util.grpc_context_manager import GRPCContextManager
from th2_grpc_common.common_pb2 import Message

logger = logging.getLogger()


StatusMessagesDict = Dict[int, List[Message]]


class CacheFilter:
    """Contains methods to get messages from the cache."""

    def __init__(self, context_manager: GRPCContextManager, cache: Cache) -> None:
        self.context_manager = context_manager

        self.cache: Cache = cache
        self.last_filtered_message_index: Optional[int] = None

    def get_n_matching_within_timeout(self,
                                      message_filters: Dict[Callable, int],
                                      n: int,
                                      check_previous_messages: bool,
                                      timeout: Union[int, float],
                                      start_time: float) -> Tuple[StatusMessagesDict, Optional[int]]:
        """Returns first N received messages that match the message_filters from cache.

        The method will check cache until all N messages are found or until context is no more active
        or until timeout is over.
        """

        status_messages_dict: Dict[int, List[Message]] = defaultdict(list)
        matching_message_index: Optional[int] = None
        start_index = self._get_start_index(check_previous_messages)
        messages_to_receive = n

        while messages_to_receive > 0:
            if self.context_manager.is_context_active() and time() - start_time < timeout:
                stop_index = self.cache.size

                status_message_iterator = self.get_filtered_message_iterator(message_filters=message_filters,
                                                                             messages_to_receive=messages_to_receive,
                                                                             start_index=start_index,
                                                                             stop_index=stop_index)

                for index, status, message in status_message_iterator:
                    status_messages_dict[status].append(message)
                    matching_message_index = index
                    messages_to_receive -= 1

                start_index = stop_index
            else:
                break

        self._update_last_filtered_message_index(index=matching_message_index)

        return status_messages_dict, matching_message_index

    def get_n_matching_immediately(self,
                                   message_filters: Dict[Callable, int],
                                   check_previous_messages: bool,
                                   n: Optional[int] = None) -> Tuple[StatusMessagesDict, Optional[int]]:
        """Returns all received messages that match the message_filter from cache.

        The method will check cache only once.
        """

        status_messages_dict: Dict[int, List[Message]] = defaultdict(list)
        matching_message_index: Optional[int] = None
        start_index = self._get_start_index(check_previous_messages)

        if self.context_manager.is_context_active():
            status_message_iterator = self.get_filtered_message_iterator(message_filters=message_filters,
                                                                         messages_to_receive=n,
                                                                         start_index=start_index,
                                                                         stop_index=self.cache.size)
            for index, status, message in status_message_iterator:
                status_messages_dict[status].append(message)
                matching_message_index = index

        self._update_last_filtered_message_index(index=matching_message_index)

        return status_messages_dict, matching_message_index

    def get_filtered_message_iterator(self,
                                      message_filters: Dict[Callable, int],
                                      messages_to_receive: Optional[int],
                                      start_index: int,
                                      stop_index: int) -> Iterator[Tuple[int, int, Message]]:
        return islice(
            (
                (index, status, message)
                for index, message in islice(enumerate(self.cache), start_index, stop_index)
                for condition, status in message_filters.items()
                if check(condition, message)
            ),
            0,
            messages_to_receive
        )

    def get_all_before_matching_from_cache(self, index: Optional[int]) -> List[Message]:
        if index is not None:
            return self.cache[0:index + 1]  # type: ignore
        else:
            return []

    def get_all_after_matching_from_cache(self, index: Optional[int]) -> List[Message]:
        if index is not None:
            return self.cache[index:self.cache.size]  # type: ignore
        else:
            return []

    def _get_start_index(self, check_previous_messages: bool) -> int:
        if check_previous_messages or self.last_filtered_message_index is None:
            return 0
        else:
            return self.last_filtered_message_index

    def _update_last_filtered_message_index(self, index: Optional[int]) -> None:
        if index is not None:
            self.last_filtered_message_index = index
