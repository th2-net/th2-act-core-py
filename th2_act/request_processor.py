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
from time import time
import traceback
from typing import Any, Callable, Dict, List, Optional, Union

from th2_act.act_response import ActMultiResponse, ActResponse
from th2_act.grpc_method_attributes import GrpcMethodAttributes
from th2_act.handler_attributes import HandlerAttributes
from th2_act.util.act_receiver import ActReceiver
from th2_act.util.act_sender import ActSender
from th2_act.util.cache import Cache
from th2_act.util.cache_filter import CacheFilter
from th2_act.util.grpc_context_manager import GRPCContextManager
from th2_act.util.request_processor_utils import RequestProcessorUtils
from th2_grpc_common.common_pb2 import Direction, Message, RequestStatus

logger = logging.getLogger()


class RequestProcessor:
    """A context manager used for sending requests and filtering messages from system under test.

    Mainly used when implementing gRPC methods in ActHandler class.

    Args:
        handler_attrs (HandlerAttributes): HandlerAttributes class instance. Generates by Act when loading handlers.
        method_attrs (GrpcMethodAttributes): GrpcMethodAttributes class instance. Initialize it with gRPC context,
            request event ID, gRPC method name and request description.
        prefilter (lambda or function, optional): Filter for messages that will be placed in cache.
            Defaults to None - cache will collect all messages.
    """

    def __init__(self,
                 handler_attrs: HandlerAttributes,
                 method_attrs: GrpcMethodAttributes,
                 prefilter: Optional[Callable] = None):
        self._subscription_manager = handler_attrs.subscription_manager
        self._grpc_context_manager = GRPCContextManager(context=method_attrs.context)

        self._cache: Cache = Cache()
        self._cache_filter: CacheFilter = CacheFilter(context_manager=self._grpc_context_manager, cache=self._cache)
        self._act_receiver: ActReceiver = ActReceiver(prefilter=prefilter, cache=self._cache)

        self._act_sender = ActSender(message_router=handler_attrs.message_router,
                                     event_router=handler_attrs.event_router,
                                     grpc_context_manager=self._grpc_context_manager,
                                     check1_connector=handler_attrs.check1_connector,
                                     method_attrs=method_attrs)
        self._checkpoint = self._act_sender.checkpoint

        self._rp_utils = RequestProcessorUtils(act_sender=self._act_sender)

    def __enter__(self) -> Any:
        self._subscription_manager.register(message_listener=self._act_receiver.message_listener)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Optional[bool]:
        try:
            self._subscription_manager.unregister(message_listener=self._act_receiver.message_listener)
            self.clear_cache()
            if exc_type:
                logger.error(f'Exception occurred in RequestProcessor: {exc_type}'
                             f'\n{"".join(traceback.format_tb(exc_tb))}')
                return True
            return None
        except Exception as e:
            logger.error(f'Could not stop subscriber monitor: {e}\n{"".join(traceback.format_tb(e.__traceback__))}')
            return None

    def send(self,
             message: Message,
             echo_key_field: Optional[str] = None,
             timeout: Optional[Union[int, float]] = None) -> Optional[str]:
        """Sends message to conn. Returns 'MsgSeqNum' field of sent message's echo if 'echo_key_field' is set,
        else None.

        Args:
            message (Message): Message to send.
            echo_key_field (str): Key field to catch sent message's echo. If not set, no echo will be returned.
            timeout (int or float, optional): The time (in seconds) during which you are ready to wait for the echo.
                The echo will be returned as soon as it is found, without waiting for the end of the timeout.
                If not set, the gRPC context time will be used.

        Returns:
            'MsgSeqNum' if 'echo_key_field' is set, else None.
        """
        if not self._act_sender.send_message(message):
            self._grpc_context_manager.cancel_context()
            logger.debug('Context was cancelled because of message sending fail')
            return None

        if echo_key_field is not None:
            echo_message = self._receive_request_message_echo(message, echo_key_field, timeout)
            if echo_message is not None:
                return echo_message.fields['header'].message_value.fields['MsgSeqNum'].simple_value

        return None

    def receive_first_matching(self,
                               message_filters: Dict[Callable, int],
                               timeout: Optional[Union[int, float]] = None,
                               check_previous_messages: bool = True) -> ActResponse:
        """Returns the first message (wrapped in ActResponse class) from cache that matches one of the filters.
        If no message found, None will be returned.

        Args:
            message_filters (Dict[lambda, RequestStatus]): The filter (lambda or function) through which the messages
                from cache will be filtered out - as key, RequestStatus (SUCCESS or ERROR) - as value.
            timeout (int or float, optional): The time (in seconds) during which you are ready to wait for the message.
                The message will be returned as soon as it is found, without waiting for the end of the timeout.
                If not set, the gRPC context time will be used.
            check_previous_messages (bool, optional): Set True if you want to look for the message throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            ActResponse class instance with received message, status and checkpoint, if any message found.
        """

        start_time = time()

        if timeout is None:
            timeout = self._grpc_context_manager.context_time_remaining()

        status_messages_dict, _ = self._cache_filter.get_n_matching_within_timeout(
            message_filters=message_filters,
            n=1,
            check_previous_messages=check_previous_messages,
            timeout=timeout,
            start_time=start_time
        )

        runtime = time() - start_time

        act_responses = self._rp_utils.create_act_responses_list(status_messages_dict=status_messages_dict)
        self._rp_utils.create_and_send_responses_events(receive_method_name='first matching',
                                                        runtime=runtime,
                                                        act_responses=act_responses)

        if act_responses is not None:
            return act_responses[0]

        return ActResponse(status=RequestStatus.ERROR, checkpoint=self._checkpoint)

    def receive_first_n_matching(self,
                                 message_filters: Dict[Callable, int],
                                 n: int,
                                 timeout: Optional[Union[int, float]] = None,
                                 check_previous_messages: bool = True) -> List[ActResponse]:
        """Returns a list of first N messages (each wrapped in ActResponse class) from cache that match
        the filters. If no messages found, None will be returned.

        Args:
            message_filters (Dict[lambda, RequestStatus]): The filter (lambda or function) through which the messages
                from cache will be filtered out - as key, RequestStatus (SUCCESS or ERROR) - as value.
            n (int): A number of filtered messages you want to receive.
            timeout (float, optional): The time (in seconds) during which you are ready to wait for the messages.
                The messages will be returned as soon as N messages are found, without waiting for the end of the
                timeout. If not set, the gRPC context time will be used.
            check_previous_messages (bool, optional): Set True if you want to look for the message throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            List of ActResponse class instances with received message, status and checkpoint, if any message found.
        """

        start_time = time()

        if timeout is None:
            timeout = self._grpc_context_manager.context_time_remaining()

        status_messages_dict, _ = self._cache_filter.get_n_matching_within_timeout(
            message_filters=message_filters,
            n=n,
            check_previous_messages=check_previous_messages,
            timeout=timeout,
            start_time=start_time
        )

        runtime = time() - start_time

        act_responses = self._rp_utils.create_act_responses_list(status_messages_dict=status_messages_dict)
        self._rp_utils.create_and_send_responses_events(receive_method_name='first n matching',
                                                        act_responses=act_responses,
                                                        number_of_responses_expected=n,
                                                        runtime=runtime)

        return act_responses or [ActResponse(status=RequestStatus.ERROR, checkpoint=self._checkpoint)]

    def receive_all_before_matching(self,
                                    message_filters: Dict[Callable, int],
                                    timeout: Optional[Union[int, float]] = None,
                                    check_previous_messages: bool = True) -> ActMultiResponse:
        """Returns an ActMultiResponse with messages from cache that precede a message that
        matches one of the filters. Message that matching condition is included in the list.
        If no messages found, None will be returned.

        Args:
            message_filters (Dict[lambda, RequestStatus]): The filter (lambda or function) through which the messages
                from cache will be filtered out - as key, RequestStatus (SUCCESS or ERROR) - as value.
            timeout (float, optional): The time (in seconds) during which you are ready to wait for the messages.
                The messages will be returned as soon as the first matching message is found,
                without waiting for the end of the timeout. If not set, the gRPC context time will be used.
            check_previous_messages (bool, optional): Set True if you want to look for the messages throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            List of ActResponse class instances with received message, status and checkpoint, if any message found.
        """

        start_time = time()

        if timeout is None:
            timeout = self._grpc_context_manager.context_time_remaining()

        status_messages_dict, index = self._cache_filter.get_n_matching_within_timeout(
            message_filters=message_filters,
            n=1,
            check_previous_messages=check_previous_messages,
            timeout=timeout,
            start_time=start_time
        )

        runtime = time() - start_time

        for status in status_messages_dict:
            status_messages_dict[status] = self._cache_filter.get_all_before_matching_from_cache(index=index)
        act_multi_response = self._rp_utils.create_act_multi_response(status_messages_dict=status_messages_dict)
        self._rp_utils.create_and_send_responses_events(receive_method_name='all before matching',
                                                        act_responses=act_multi_response,
                                                        runtime=runtime)

        return act_multi_response or ActMultiResponse(status=RequestStatus.ERROR, checkpoint=self._checkpoint)

    def receive_all_after_matching(self,
                                   message_filters: Dict[Callable, int],
                                   wait_time: Optional[Union[int, float]] = None,
                                   check_previous_messages: bool = True) -> ActMultiResponse:
        """Returns a ActMultiResponse with messages from cache that follows a message that
        matches one of the filters. Message that matching condition is included in the list.
        If no messages found, None will be returned.

        Args:
            message_filters (Dict[lambda, RequestStatus]): The filter (lambda or function) through which the messages
                from cache will be filtered out - as key, RequestStatus (SUCCESS or ERROR) - as value.
            wait_time (int, optional): The time (in seconds) that the Act will wait before checking the cache for
                the matching message. Defaults to None (cache will be checked immediately and only once).
            check_previous_messages (bool, optional): Set True if you want to look for the message throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            List of ActResponse class instances with received message, status and checkpoint, if any message found.
       """

        start_time = time()

        if wait_time:
            self._grpc_context_manager.wait(wait_time)

        status_messages_dict, index = self._cache_filter.get_n_matching_immediately(
            message_filters=message_filters,
            check_previous_messages=check_previous_messages,
            n=1
        )

        runtime = time() - start_time

        for status in status_messages_dict:
            status_messages_dict[status] = self._cache_filter.get_all_after_matching_from_cache(index=index)
        act_multi_response = self._rp_utils.create_act_multi_response(status_messages_dict=status_messages_dict)
        self._rp_utils.create_and_send_responses_events(receive_method_name='all after matching',
                                                        act_responses=act_multi_response,
                                                        runtime=runtime)

        return act_multi_response or ActMultiResponse(status=RequestStatus.ERROR, checkpoint=self._checkpoint)

    def receive_all_matching(self,
                             message_filters: Dict[Callable, int],
                             wait_time: Optional[Union[int, float]] = None,
                             check_previous_messages: bool = True) -> List[ActResponse]:
        """Returns a list of messages (wrapped in ActResponse class) from cache that matches filters.
        If no messages found, None will be returned.

        Args:
            message_filters (Dict[lambda, RequestStatus]): The filter (lambda or function) through which the messages
                from cache will be filtered out - as key, RequestStatus (SUCCESS or ERROR) - as value.
            wait_time (int, optional): The time that the Act will wait before checking the cache for matching messages.
                Defaults to None (cache will be checked immediately).
            check_previous_messages (bool, optional): Set True if you want to look for the message throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            List of ActResponse class instances with received message, status and checkpoint, if any message found.
        """

        start_time = time()

        if wait_time:
            self._grpc_context_manager.wait(wait_time)

        status_messages_dict, index = self._cache_filter.get_n_matching_immediately(
            message_filters=message_filters,
            check_previous_messages=check_previous_messages
        )

        runtime = time() - start_time

        act_responses = self._rp_utils.create_act_responses_list(status_messages_dict=status_messages_dict)
        self._rp_utils.create_and_send_responses_events(receive_method_name='all matching',
                                                        act_responses=act_responses,
                                                        runtime=runtime)

        return act_responses or [ActResponse(status=RequestStatus.ERROR, checkpoint=self._checkpoint)]

    @property
    def cache_size(self) -> int:
        """Returns a number of messages in cache."""

        return self._cache.size

    def clear_cache(self) -> None:
        """Deletes all messages from cache."""

        self._cache.clear()

    def _receive_request_message_echo(self,
                                      message: Message,
                                      echo_key_field: str,
                                      timeout: Optional[Union[int, float]]) -> Optional[Message]:
        start_time = time()

        if timeout is None:
            timeout = self._grpc_context_manager.context_time_remaining()

        echo_filter = lambda echo: (  # noqa: E731, ECE001
                echo.fields[echo_key_field] == message.fields[echo_key_field]
                and echo.metadata.message_type == message.metadata.message_type
                and echo.metadata.id.direction == Direction.SECOND
                and echo.metadata.id.connection_id.session_alias == message.metadata.id.connection_id.session_alias
        )

        status_messages_dict, _ = self._cache_filter.get_n_matching_within_timeout(
            message_filters={echo_filter: RequestStatus.SUCCESS},
            n=1,
            check_previous_messages=False,
            timeout=timeout,
            start_time=start_time
        )

        act_responses = self._rp_utils.create_act_responses_list(status_messages_dict=status_messages_dict)

        if act_responses is not None:
            return act_responses[0].message

        logger.error(f'Cannot find echo with key field {echo_key_field} of message '
                     f'with type {message.metadata.message_type} and '
                     f'session alias {message.metadata.id.connection_id.session_alias}')
        return None
