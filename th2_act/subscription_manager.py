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
from typing import Set

from th2_common.schema.message.message_listener import MessageListener
from th2_grpc_common.common_pb2 import MessageBatch

logger = logging.getLogger()


class SubscriptionManager(MessageListener):

    def __init__(self) -> None:
        self.listeners: Set[MessageListener] = set()

    def register(self, message_listener: MessageListener) -> None:
        self.listeners.add(message_listener)

    def unregister(self, message_listener: MessageListener) -> None:
        self.listeners.remove(message_listener)

    def handler(self, consumer_tag: str, message_batch: MessageBatch) -> None:
        for listener in self.listeners:
            try:
                listener.handler(consumer_tag, message_batch)
            except Exception as e:
                logger.error('Cannot handle batch: %s' % e)
