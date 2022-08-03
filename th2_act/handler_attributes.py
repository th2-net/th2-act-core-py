#   Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

from th2_act.util.subscription_manager import SubscriptionManager
from th2_common.schema.message.message_router import MessageRouter
from th2_grpc_check1.check1_service import Check1Service


class HandlerAttributes:
    """Contains parameters of ActHandler implementation. HandlerAttributes are used in RequestProcessor
    context manager. HandlerAttributes are created automatically when loading handlers.
    """

    __slots__ = ('check1_connector', 'subscription_manager', 'message_router', 'event_router')

    def __init__(self,
                 message_router: MessageRouter,
                 event_router: MessageRouter,
                 subscription_manager: SubscriptionManager,
                 check1_connector: Check1Service):
        self.message_router = message_router
        self.event_router = event_router
        self.check1_connector = check1_connector
        self.subscription_manager = subscription_manager
