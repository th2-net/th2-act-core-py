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

from itertools import islice
from time import time
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Union

from th2_act import ActMessage
from th2_act.util.act_receiver import check
from th2_act.util.cache import Cache
from th2_act.util.grpc_context_manager import GRPCContextManager
from th2_grpc_common.common_pb2 import RequestStatus


class CacheFilter:
    MessageFiltersType = Union[Callable, Iterable[Callable]]

    def __init__(self, cache: Cache, grpc_context_manager: GRPCContextManager):
        self.cache = cache
        self.grpc_context_manager = grpc_context_manager
        self.last_filtered_message_index: Optional[int] = None

    def filter_single_message_immediately(
            self,
            check_previous_messages: bool,
            pass_on: Optional[MessageFiltersType] = None,
            fail_on: Optional[MessageFiltersType] = None) -> Tuple[Optional[int], ActMessage, float]:
        index, act_message = None, ActMessage()
        start_time = time()

        if self.grpc_context_manager.is_context_active():
            filtered_messages_slice = self.get_filtered_message_slice(
                message_filters=self._create_message_filters(pass_on=pass_on, fail_on=fail_on),
                messages_to_receive=1,
                start_index=self._get_start_index(check_previous_messages)
            )

            index, act_message = next(filtered_messages_slice, (index, act_message))

        runtime = time() - start_time
        self._update_last_filtered_message_index(index=index)

        return index, act_message, runtime

    def filter_single_message_within_timeout(
            self,
            timeout: Optional[float],
            check_previous_messages: bool,
            pass_on: Optional[MessageFiltersType] = None,
            fail_on: Optional[MessageFiltersType] = None) -> Tuple[Optional[int], ActMessage, float]:
        index, act_message = None, ActMessage()
        if timeout is None:
            timeout = self.grpc_context_manager.context_time_remaining()

        start_time = time()
        is_time_left = lambda: time() - start_time < timeout

        while index is None and is_time_left():
            if self.grpc_context_manager.is_context_active():
                filtered_messages_slice = self.get_filtered_message_slice(
                    message_filters=self._create_message_filters(pass_on=pass_on, fail_on=fail_on),
                    messages_to_receive=1,
                    start_index=self._get_start_index(check_previous_messages)
                )

                index, act_message = next(filtered_messages_slice, (index, act_message))
            else:
                break

        runtime = time() - start_time
        self._update_last_filtered_message_index(index=index)

        return index, act_message, runtime

    def filter_list_messages_immediately(
            self,
            check_previous_messages: bool,
            messages_to_receive: Optional[int] = None,
            pass_on: Optional[MessageFiltersType] = None,
            fail_on: Optional[MessageFiltersType] = None) -> Tuple[Optional[int], List[ActMessage], float]:
        index, act_messages = None, []
        start_time = time()

        if self.grpc_context_manager.is_context_active():
            filtered_messages_slice = self.get_filtered_message_slice(
                message_filters=self._create_message_filters(pass_on=pass_on, fail_on=fail_on),
                messages_to_receive=messages_to_receive,
                start_index=self._get_start_index(check_previous_messages)
            )

            for i, act_message in filtered_messages_slice:
                act_messages.append(act_message)
                index = i

        runtime = time() - start_time
        self._update_last_filtered_message_index(index=index)

        return index, act_messages, runtime

    def filter_list_messages_within_timeout(
            self,
            check_previous_messages: bool,
            messages_to_receive: int,
            timeout: Optional[float] = None,
            pass_on: Optional[MessageFiltersType] = None,
            fail_on: Optional[MessageFiltersType] = None) -> Tuple[Optional[int], List[ActMessage], float]:
        index, act_messages = None, []
        messages_to_filter = messages_to_receive
        if timeout is None:
            timeout = self.grpc_context_manager.context_time_remaining()

        start_time = time()
        is_time_left = lambda: time() - start_time < timeout

        while messages_to_filter > 0 and is_time_left():
            if self.grpc_context_manager.is_context_active():
                filtered_messages_slice = self.get_filtered_message_slice(
                    message_filters=self._create_message_filters(pass_on=pass_on, fail_on=fail_on),
                    messages_to_receive=messages_to_receive,
                    start_index=self._get_start_index(check_previous_messages)
                )

                for i, act_message in filtered_messages_slice:
                    act_messages.append(act_message)
                    messages_to_filter -= 1
                    index = i
            else:
                break

        runtime = time() - start_time
        self._update_last_filtered_message_index(index=index)

        return index, act_messages, runtime

    def get_filtered_message_slice(self,
                                   message_filters: Dict[Callable, int],
                                   messages_to_receive: Optional[int],
                                   start_index: int) -> Iterator[Tuple[int, ActMessage]]:
        return islice(
            (
                (index, ActMessage(message=message, status=status))
                for index, message in islice(enumerate(self.cache), start_index, self.cache.size)
                for condition, status in message_filters.items()
                if check(condition, message)
            ),
            messages_to_receive
        )

    def _create_message_filters(self,
                                pass_on: Optional[MessageFiltersType],
                                fail_on: Optional[MessageFiltersType]) -> Dict[Callable, int]:
        message_filters: Dict[Callable, int] = {}
        if isinstance(pass_on, Callable):  # type: ignore
            message_filters[pass_on] = RequestStatus.SUCCESS  # type: ignore
        elif isinstance(pass_on, Iterable):
            message_filters.update({message_filter: RequestStatus.SUCCESS for message_filter in pass_on})

        if isinstance(fail_on, Callable):  # type: ignore
            message_filters[fail_on] = RequestStatus.ERROR  # type: ignore
        elif isinstance(fail_on, Iterable):
            message_filters.update({message_filter: RequestStatus.ERROR for message_filter in fail_on})

        return message_filters

    def _get_start_index(self, check_previous_messages: bool) -> int:
        if check_previous_messages or self.last_filtered_message_index is None:
            return 0
        else:
            return self.last_filtered_message_index

    def _update_last_filtered_message_index(self, index: Optional[int]) -> None:
        if index is not None:
            self.last_filtered_message_index = index
