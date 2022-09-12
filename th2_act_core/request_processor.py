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
from typing import Any, Callable, Iterable, List, Optional, Union

from th2_act_core.act_message import ActMessage, ActMessageUtil
from th2_act_core.grpc_method_attributes import GrpcMethodAttributes
from th2_act_core.handler_attributes import HandlerAttributes
from th2_act_core.util.act_receiver import ActReceiver
from th2_act_core.util.act_sender import ActSender
from th2_act_core.util.cache import Cache
from th2_act_core.util.cache_filter import CacheFilter
from th2_act_core.util.grpc_context_manager import GRPCContextManager
from th2_grpc_common.common_pb2 import Direction, EventID, Message, RequestStatus

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

    MessageFiltersType = Union[Callable, Iterable[Callable]]

    def __init__(self,
                 handler_attrs: HandlerAttributes,
                 method_attrs: GrpcMethodAttributes,
                 prefilter: Optional[Callable] = None):
        self._subscription_manager = handler_attrs.subscription_manager
        self._grpc_context_manager = GRPCContextManager(context=method_attrs.context)

        self._cache: Cache = Cache()
        self._act_receiver: ActReceiver = ActReceiver(prefilter=prefilter, cache=self._cache)

        self._act_sender = ActSender(message_router=handler_attrs.message_router,
                                     event_router=handler_attrs.event_router,
                                     grpc_context_manager=self._grpc_context_manager,
                                     check1_connector=handler_attrs.check1_connector,
                                     method_attrs=method_attrs)
        self.checkpoint = self._act_sender.register_checkpoint()

        self.cache_filter = CacheFilter(cache=self._cache,
                                        grpc_context_manager=self._grpc_context_manager)

    def __enter__(self) -> Any:
        self._subscription_manager.register(message_listener=self._act_receiver.message_listener)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Optional[bool]:
        try:
            self._subscription_manager.unregister(message_listener=self._act_receiver.message_listener)
            self.clear_cache()
            if exc_type:
                logger.error(f'{exc_type} exception occurred in RequestProcessor: {exc_val}'
                             f'\n{"".join(traceback.format_tb(exc_tb))}')
                return True
            return None
        except Exception as e:
            logger.error(f'Could not stop subscriber monitor: {e}\n{"".join(traceback.format_tb(e.__traceback__))}')
            return None

    def send(self,
             message: Message,
             echo_key_field: Optional[str] = None,
             timeout: Optional[Union[int, float]] = None) -> Optional[ActMessage]:
        """Sends message to conn. Returns sent message's echo if 'echo_key_field' is set, else None.

        Args:
            message (Message): Message to send.
            echo_key_field (str): Key field to catch sent message's echo. If not set, no echo will be returned.
            timeout (int or float, optional): The time (in seconds) during which you are ready to wait for the echo.
                The echo will be returned as soon as it is found, without waiting for the end of the timeout.
                If not set, the gRPC context time will be used.

        Returns:
            Sent message's echo (as ActMessage) or None.
        """
        if not self._act_sender.send_message(message):
            self._grpc_context_manager.cancel_context()
            logger.info('Context was cancelled because of message sending fail')
            return None

        if echo_key_field is not None:
            return self._receive_request_message_echo(message, echo_key_field, timeout)

        return None

    def receive_first_matching(self, *,
                               pass_on: Optional[Union[Callable, Iterable[Callable]]] = None,
                               fail_on: Optional[Union[Callable, Iterable[Callable]]] = None,
                               timeout: Optional[Union[int, float]] = None,
                               check_previous_messages: bool = True) -> ActMessage:
        """Returns the first message (wrapped in ActMessage class) from cache that matches one of the
        filters.

        Args:
            pass_on (lambda or Iterable[lambda]): The filter (lambda or function) through which the messages
                from cache will be filtered out with RequestStatus.SUCCESS.
            fail_on (lambda or Iterable[lambda]): The filter (lambda or function) through which the messages
                from cache will be filtered out with RequestStatus.ERROR.
            timeout (int or float, optional): The time (in seconds) during which you are ready to wait for the message.
                The message will be returned as soon as it is found, without waiting for the end of the timeout.
                If not set, the gRPC context time will be used.
            check_previous_messages (bool, optional): Set True if you want to look for the message throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            ActMessage class instance with received message and status. If no message found,
            ActMessage's 'message' field will be None.
        """

        _, act_message, runtime = self.cache_filter.filter_single_message_within_timeout(
            pass_on=pass_on,
            fail_on=fail_on,
            check_previous_messages=check_previous_messages,
            timeout=timeout
        )

        self._create_and_send_responses_events(receive_method_name='first matching',
                                               runtime=runtime,
                                               act_messages=act_message)
        return act_message

    def receive_first_n_matching(self, *,
                                 n: int,
                                 pass_on: Optional[Union[Callable, Iterable[Callable]]] = None,
                                 fail_on: Optional[Union[Callable, Iterable[Callable]]] = None,
                                 timeout: Optional[Union[int, float]] = None,
                                 check_previous_messages: bool = True) -> List[ActMessage]:
        """Returns a list of first N messages (each wrapped in ActMessage class) from cache that
        match the filters.

        Args:
            pass_on (lambda or Iterable[lambda]): The filter (lambda or function) through which the messages
                from cache will be filtered out with RequestStatus.SUCCESS.
            fail_on (lambda or Iterable[lambda]): The filter (lambda or function) through which the messages
                from cache will be filtered out with RequestStatus.ERROR.
            n (int): A number of filtered messages you want to receive.
            timeout (float, optional): The time (in seconds) during which you are ready to wait for the messages.
                The messages will be returned as soon as N messages are found, without waiting for the end of the
                timeout. If not set, the gRPC context time will be used.
            check_previous_messages (bool, optional): Set True if you want to look for the message throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            A list of ActMessage. If no messages found, empty list will be returned.
        """

        _, act_messages, runtime = self.cache_filter.filter_list_messages_within_timeout(
            pass_on=pass_on,
            fail_on=fail_on,
            timeout=timeout,
            messages_to_receive=n,
            check_previous_messages=check_previous_messages
        )

        self._create_and_send_responses_events(receive_method_name='first n matching',
                                               act_messages=act_messages,
                                               number_of_responses_expected=n,
                                               runtime=runtime)

        return act_messages

    def receive_all_before_matching(self, *,
                                    pass_on: Optional[Union[Callable, Iterable[Callable]]] = None,
                                    fail_on: Optional[Union[Callable, Iterable[Callable]]] = None,
                                    timeout: Optional[Union[int, float]] = None,
                                    check_previous_messages: bool = True) -> List[ActMessage]:
        """Returns a list of messages (each wrapped in ActMessage class) from cache that precede a message
        that matches one of the filters. Message that matching the filter is included in the list.

        Args:
            pass_on (lambda or Iterable[lambda]): The filter (lambda or function) through which the messages
                from cache will be filtered out with RequestStatus.SUCCESS.
            fail_on (lambda or Iterable[lambda]): The filter (lambda or function) through which the messages
                from cache will be filtered out with RequestStatus.ERROR.
            timeout (float, optional): The time (in seconds) during which you are ready to wait for the messages.
                The messages will be returned as soon as the first matching message is found,
                without waiting for the end of the timeout. If not set, the gRPC context time will be used.
            check_previous_messages (bool, optional): Set True if you want to look for the messages throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            A list of ActMessage. If no messages found, empty list will be returned.
        """

        index, act_message, runtime = self.cache_filter.filter_single_message_within_timeout(
            pass_on=pass_on,
            fail_on=fail_on,
            check_previous_messages=check_previous_messages,
            timeout=timeout
        )

        th2_messages = self._cache.get_all_before_matching_from_cache(index=index)
        act_messages = [ActMessage(message=message, status=act_message.status) for message in th2_messages]
        self._create_and_send_responses_events(receive_method_name='all before matching',
                                               act_messages=act_messages,
                                               runtime=runtime)

        return act_messages

    def receive_all_after_matching(self, *,
                                   pass_on: Optional[Union[Callable, Iterable[Callable]]] = None,
                                   fail_on: Optional[Union[Callable, Iterable[Callable]]] = None,
                                   wait_time: Optional[Union[int, float]] = None,
                                   check_previous_messages: bool = True) -> List[ActMessage]:
        """Returns a list of messages (each wrapped in ActMessage class) from cache that follows a message
        that matches one of the filters. Message that matching the filter is included in the list.

        Args:
            pass_on (lambda or Iterable[lambda]): The filter (lambda or function) through which the messages
                from cache will be filtered out with RequestStatus.SUCCESS.
            fail_on (lambda or Iterable[lambda]): The filter (lambda or function) through which the messages
                from cache will be filtered out with RequestStatus.ERROR.
            wait_time (int, optional): The time (in seconds) that the Act will wait before checking the cache for
                the matching message. Defaults to None (cache will be checked immediately and only once).
            check_previous_messages (bool, optional): Set True if you want to look for the message throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            A list of ActMessage. If no messages found, empty list will be returned.
        """

        if wait_time:
            self._grpc_context_manager.wait(wait_time)

        index, act_message, runtime = self.cache_filter.filter_single_message_immediately(
            pass_on=pass_on,
            fail_on=fail_on,
            check_previous_messages=check_previous_messages
        )

        th2_messages = self._cache.get_all_after_matching_from_cache(index=index)
        act_messages = [ActMessage(message=message, status=act_message.status) for message in th2_messages]
        self._create_and_send_responses_events(receive_method_name='all after matching',
                                               act_messages=act_messages,
                                               runtime=runtime)

        return act_messages

    def receive_all_matching(self, *,
                             pass_on: Optional[Union[Callable, Iterable[Callable]]] = None,
                             fail_on: Optional[Union[Callable, Iterable[Callable]]] = None,
                             wait_time: Optional[Union[int, float]] = None,
                             check_previous_messages: bool = True) -> List[ActMessage]:
        """Returns a list of messages (each wrapped in ActMessage class) from cache that matches filters.

        Args:
            pass_on (lambda or Iterable[lambda]): The filter (lambda or function) through which the messages
                from cache will be filtered out with RequestStatus.SUCCESS.
            fail_on (lambda or Iterable[lambda]): The filter (lambda or function) through which the messages
                from cache will be filtered out with RequestStatus.ERROR.
            wait_time (int, optional): The time that the Act will wait before checking the cache for matching messages.
                Defaults to None (cache will be checked immediately).
            check_previous_messages (bool, optional): Set True if you want to look for the message throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            A list of ActMessage. If no messages found, empty list will be returned.
        """

        if wait_time:
            self._grpc_context_manager.wait(wait_time)

        _, act_messages, runtime = self.cache_filter.filter_list_messages_immediately(
            pass_on=pass_on,
            fail_on=fail_on,
            check_previous_messages=check_previous_messages
        )

        self._create_and_send_responses_events(receive_method_name='all matching',
                                               act_messages=act_messages,
                                               runtime=runtime)

        return act_messages

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
                                      timeout: Optional[Union[int, float]]) -> Optional[ActMessage]:
        echo_filter = lambda echo: (  # noqa: E731, ECE001
                echo.fields[echo_key_field] == message.fields[echo_key_field]
                and echo.metadata.message_type == message.metadata.message_type
                and echo.metadata.id.direction == Direction.SECOND
                and echo.metadata.id.connection_id.session_alias == message.metadata.id.connection_id.session_alias
        )

        _, act_message, _ = self.cache_filter.filter_single_message_within_timeout(pass_on=echo_filter,
                                                                                   check_previous_messages=False,
                                                                                   timeout=timeout)

        return act_message

    def _create_and_send_responses_root_event(self,
                                              receive_method_name: str,
                                              messages_received: int = 0,
                                              messages_expected: Optional[int] = None) -> EventID:
        if messages_expected in (1, None):
            return self._act_sender.create_and_send_responses_root_event(status=RequestStatus.SUCCESS,
                                                                         event_name=f'Received {messages_received} '
                                                                                    f'message(s) - '
                                                                                    f'{receive_method_name}')
        elif messages_received == messages_expected:
            return self._act_sender.create_and_send_responses_root_event(status=RequestStatus.SUCCESS,
                                                                         event_name=f'Received {messages_received}/'
                                                                                    f'{messages_expected} message(s)'
                                                                                    f' - {receive_method_name}')
        else:
            return self._act_sender.create_and_send_responses_root_event(status=RequestStatus.ERROR,
                                                                         event_name=f'Received {messages_received}/'
                                                                                    f'{messages_expected} message(s)'
                                                                                    f' - {receive_method_name}')

    def _create_and_send_responses_events(self,
                                          receive_method_name: str,
                                          runtime: float,
                                          act_messages: Union[ActMessage, List[ActMessage]],
                                          number_of_responses_expected: Optional[int] = None) -> None:
        responses_root_event = self._create_and_send_responses_root_event(
            receive_method_name=receive_method_name,
            messages_received=ActMessageUtil.get_length(act_messages),
            messages_expected=number_of_responses_expected
        )

        self._act_sender.send_responses_events(
            responses_events=ActMessageUtil.create_events(act_messages=act_messages,
                                                          runtime=runtime,
                                                          responses_root_event=responses_root_event)
        )
