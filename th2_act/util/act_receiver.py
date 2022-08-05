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
from typing import Callable, Dict, List, Optional

from th2_act.util.cache import Cache
from th2_common.schema.message.message_listener import MessageListener
from th2_grpc_common.common_pb2 import Message, MessageBatch

logger = logging.getLogger()


StatusMessagesDict = Dict[int, List[Message]]


class ActReceiver:
    """Receives messages and put it in cache"""

    def __init__(self, prefilter: Optional[Callable], cache: Cache) -> None:
        if prefilter:
            self.prefilter = prefilter
            self.message_listener = ParsedMessageListener(self.process_incoming_messages_with_prefilter)
        else:
            self.message_listener = ParsedMessageListener(self.process_incoming_messages)

        self.cache: Cache = cache

    def process_incoming_messages(self, consumer_tag: str, message_batch: MessageBatch) -> None:
        try:
            logger.debug('Received MessageBatch with %i messages' % len(message_batch.messages))
            self.cache.add(message for message in message_batch.messages)

        except Exception as e:
            logger.error(f'Could not process incoming messages: {e}'
                         f'\n{"".join(traceback.format_tb(e.__traceback__))}')

    def process_incoming_messages_with_prefilter(self, consumer_tag: str, message_batch: MessageBatch) -> None:
        try:
            logger.debug('Received MessageBatch with %i messages' % len(message_batch.messages))
            self.cache.add(message for message in message_batch.messages if check(self.prefilter, message))

        except Exception as e:
            logger.error(f'Could not process incoming messages: {e}'
                         f'\n{"".join(traceback.format_tb(e.__traceback__))}')


class ParsedMessageListener(MessageListener):

    def __init__(self, handler_function: Callable) -> None:
        self.handler_function = handler_function

    def handler(self, consumer_tag: str, message_batch: MessageBatch) -> None:
        self.handler_function(consumer_tag, message_batch)


def check(condition: Callable[[Message], bool], message: Message) -> bool:
    try:
        return condition(message)
    except KeyError:
        return False
