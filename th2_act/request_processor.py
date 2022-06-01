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
from typing import Any, Callable, Dict, List, Optional, Union

from th2_act.act_connector import ActConnector
from th2_act.act_parameters import ActParameters
from th2_act.act_response import ActMultiResponse, ActResponse
from th2_act.cache_processor import CacheProcessor
from th2_act.request_processor_utils import RequestProcessorUtils
from th2_grpc_common.common_pb2 import Message, RequestStatus


logger = logging.getLogger()


class RequestProcessor:
    """A context manager used for sending requests and filtering messages from system under test.

    Mainly used when implementing gRPC methods in ActHandler class.

    Args:
        act_conn (ActConnector): ActConnector class instance. Generates by Act when gRPC server starts.
        act_parameters (ActParameters): ActParameters class instance. Initialize it with act name, gRPC context,
            request event ID and request description.
        prefilter (lambda or function, optional): Filter for messages that the cache will collect.
            Defaults to None - cache will collect all messages.
    """

    def __init__(self,
                 act_conn: ActConnector,
                 act_parameters: ActParameters,
                 prefilter: Optional[Callable] = None):
        self.act_name = act_parameters.act_name
        self._context = act_parameters.context

        self._cache_processor = CacheProcessor(context=self._context, prefilter=prefilter)
        self._rp_utils = RequestProcessorUtils(act_conn=act_conn,
                                               act_parameters=act_parameters,
                                               cache_processor=self._cache_processor)

        self._subscription_manager = act_conn.subscription_manager
        self._subscription_manager.register(message_listener=self._cache_processor.message_listener)

    def __enter__(self) -> Any:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Optional[bool]:
        try:
            self._subscription_manager.unregister(message_listener=self._cache_processor.message_listener)
            if exc_type:
                logger.error('Exception occurred in RequestProcessor (%s): %s. Traceback: %s'
                             % (self.act_name, exc_val, exc_tb.print_tb))
                return True
            return None
        except Exception as e:
            logger.error('Could not stop subscriber monitor: %s' % e)
            return None

    def send(self, message: Message) -> ActResponse:
        """Send message to conn.

        Args:
            message (Message): Message to send to conn.

        Returns:
            ActResponse class instance with status (SUCCESS or ERROR).
        """

        send_request_act_response = self._rp_utils.send_message(message)

        if send_request_act_response.status == RequestStatus.ERROR:
            self._context.cancel()

        return send_request_act_response

    def receive_first_matching(self,
                               message_filters: Dict[Callable, int],
                               timeout: Optional[Union[int, float]] = None,
                               check_previous_messages: bool = True) -> ActResponse:
        """Returns the first message (wrapped in ActResponse class) from cache that matches one of the conditions.

        Args:
            message_filters (Dict[lambda, RequestStatus]): The filter (lambda or function) through which the messages
                from cache will be filtered out - as key, RequestStatus (SUCCESS or ERROR) - as value.
            timeout (float, optional): The time (in seconds) during which you are ready to wait for the message.
                The message will be returned as soon as it is found, without waiting for the end of the timeout.
                If not set, the gRPC context time will be used.
            check_previous_messages (bool, optional): Set True if you want to look for the message throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            ActResponse class instance with received message, status and checkpoint.
        """

        start_time = time()

        if timeout is None:
            timeout = self._context.time_remaining()

        status_messages_dict, _ = self._cache_processor.get_n_matching_within_timeout(
            message_filters=message_filters,
            n=1,
            check_previous_messages=check_previous_messages,
            timeout=timeout,
            start_time=start_time
        )

        received_messages_root_event_id = self._rp_utils.create_received_messages_root_event(
            'first matching',
            messages_received=self._cache_processor.get_number_of_received_messages(
                status_messages_dict=status_messages_dict
            ),
            messages_expected=1
        )

        runtime = time() - start_time

        self._rp_utils.create_received_messages_events(status_messages_dict=status_messages_dict,
                                                       root_event_id=received_messages_root_event_id,
                                                       runtime=runtime)

        return self._rp_utils.create_act_responses_list(status_messages_dict=status_messages_dict,
                                                        root_event_id=received_messages_root_event_id)[0]

    def receive_first_n_matching(self,
                                 message_filters: Dict[Callable, int],
                                 n: int,
                                 timeout: Optional[Union[int, float]] = None,
                                 check_previous_messages: bool = True) -> List[ActResponse]:
        """Returns a list of first N messages (each wrapped in ActResponse class) from cache that match
        the conditions.

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
            List of ActResponse class instances with received message, status and checkpoint.
        """

        start_time = time()

        if timeout is None:
            timeout = self._context.time_remaining()

        status_messages_dict, _ = self._cache_processor.get_n_matching_within_timeout(
            message_filters=message_filters,
            n=n,
            check_previous_messages=check_previous_messages,
            timeout=timeout,
            start_time=start_time
        )

        received_messages_root_event_id = self._rp_utils.create_received_messages_root_event(
            'first n matching',
            messages_received=self._cache_processor.get_number_of_received_messages(
                status_messages_dict=status_messages_dict
            ),
            messages_expected=n
        )

        runtime = time() - start_time

        self._rp_utils.create_received_messages_events(status_messages_dict=status_messages_dict,
                                                       root_event_id=received_messages_root_event_id,
                                                       runtime=runtime)

        return self._rp_utils.create_act_responses_list(status_messages_dict=status_messages_dict,
                                                        root_event_id=received_messages_root_event_id)

    def receive_all_before_matching(self,
                                    message_filters: Dict[Callable, int],
                                    timeout: Optional[Union[int, float]] = None,
                                    check_previous_messages: bool = True) -> ActMultiResponse:
        """Returns a list of messages (each wrapped in ActResponse class) from cache that precede a message that
        matches one of the conditions. Message that matching condition is included in the list.

        Args:
            message_filters (Dict[lambda, RequestStatus]): The filter (lambda or function) through which the messages
                from cache will be filtered out - as key, RequestStatus (SUCCESS or ERROR) - as value.
            timeout (float, optional): The time (in seconds) during which you are ready to wait for the messages.
                The messages will be returned as soon as the first matching message is found,
                without waiting for the end of the timeout. If not set, the gRPC context time will be used.
            check_previous_messages (bool, optional): Set True if you want to look for the messages throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            List of ActResponse class instances with received message, status and checkpoint.
        """

        start_time = time()

        if timeout is None:
            timeout = self._context.time_remaining()

        status_messages_dict, index = self._cache_processor.get_n_matching_within_timeout(
            message_filters=message_filters,
            n=1,
            check_previous_messages=check_previous_messages,
            timeout=timeout,
            start_time=start_time
        )

        received_messages_root_event_id = self._rp_utils.create_received_messages_root_event(
            'all before matching',
            messages_received=self._cache_processor.get_number_of_received_messages(before_index=index)
        )

        for status in status_messages_dict:
            status_messages_dict[status] = self._cache_processor.get_all_before_matching_from_cache(index=index)

        runtime = time() - start_time

        self._rp_utils.create_received_messages_events(status_messages_dict=status_messages_dict,
                                                       root_event_id=received_messages_root_event_id,
                                                       runtime=runtime)

        return self._rp_utils.create_act_multi_response(status_messages_dict=status_messages_dict,
                                                        root_event_id=received_messages_root_event_id)

    def receive_all_after_matching(self,
                                   message_filters: Dict[Callable, int],
                                   wait_time: Optional[Union[int, float]] = None,
                                   check_previous_messages: bool = True) -> ActMultiResponse:
        """Returns a list of messages (each wrapped in ActResponse class) from cache that follows a message that
        matches one of the conditions. Message that matching condition is included in the list.

        Args:
            message_filters (Dict[lambda, RequestStatus]): The filter (lambda or function) through which the messages
                from cache will be filtered out - as key, RequestStatus (SUCCESS or ERROR) - as value.
            wait_time (int, optional): The time (in seconds) that the Act will wait before checking the cache for
                the matching message. Defaults to None (cache will be checked immediately and only once).
            check_previous_messages (bool, optional): Set True if you want to look for the message throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            List of ActResponse class instances with received message, status and checkpoint.
       """

        start_time = time()

        if wait_time:
            self._rp_utils.wait(wait_time)

        status_messages_dict, index = self._cache_processor.get_n_matching_immediately(
            message_filters=message_filters,
            check_previous_messages=check_previous_messages,
            n=1
        )

        received_messages_root_event_id = self._rp_utils.create_received_messages_root_event(
            'all after matching',
            messages_received=self._cache_processor.get_number_of_received_messages(after_index=index)
        )

        for status in status_messages_dict:
            status_messages_dict[status] = self._cache_processor.get_all_after_matching_from_cache(index=index)

        runtime = time() - start_time

        self._rp_utils.create_received_messages_events(status_messages_dict=status_messages_dict,
                                                       root_event_id=received_messages_root_event_id,
                                                       runtime=runtime)

        return self._rp_utils.create_act_multi_response(status_messages_dict=status_messages_dict,
                                                        root_event_id=received_messages_root_event_id)

    def receive_all_matching(self,
                             message_filters: Dict[Callable, int],
                             wait_time: Optional[Union[int, float]] = None,
                             check_previous_messages: bool = True) -> List[ActResponse]:
        """Returns a list of messages (wrapped in ActResponse class) from cache that matches conditions.

        Args:
            message_filters (Dict[lambda, RequestStatus]): The filter (lambda or function) through which the messages
                from cache will be filtered out - as key, RequestStatus (SUCCESS or ERROR) - as value.
            wait_time (int, optional): The time that the Act will wait before checking the cache for matching messages.
                Defaults to None (cache will be checked immediately).
            check_previous_messages (bool, optional): Set True if you want to look for the message throughout all
                received messages; set False if you want to check only newly received messages. Defaults to True.

        Returns:
            List of ActResponse class instances with received message, status and checkpoint.
        """

        start_time = time()

        if wait_time:
            self._rp_utils.wait(wait_time)

        status_messages_dict, index = self._cache_processor.get_n_matching_immediately(
            message_filters=message_filters,
            check_previous_messages=check_previous_messages
        )

        received_messages_root_event_id = self._rp_utils.create_received_messages_root_event(
            'all matching',
            messages_received=self._cache_processor.get_number_of_received_messages(
                status_messages_dict=status_messages_dict
            )
        )

        runtime = time() - start_time

        self._rp_utils.create_received_messages_events(status_messages_dict=status_messages_dict,
                                                       root_event_id=received_messages_root_event_id,
                                                       runtime=runtime)

        return self._rp_utils.create_act_responses_list(status_messages_dict=status_messages_dict,
                                                        root_event_id=received_messages_root_event_id)

    @property
    def cache_size(self) -> int:
        """Returns a number of messages in cache."""

        return self._cache_processor.cache_size

    def clear_cache(self) -> None:
        """Deletes all messages from cache."""

        self._cache_processor.clear_cache()
