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
import threading
from typing import Union

from grpc import RpcContext

logger = logging.getLogger()


class GRPCContextManager:

    def __init__(self, context: RpcContext):
        self.context = context

        self.time_lock = threading.RLock()
        self.time_condition = threading.Condition(self.time_lock)

    def context_time_remaining(self) -> float:
        return self.context.time_remaining()  # type: ignore

    def is_context_active(self) -> bool:
        return self.context.is_active()  # type: ignore

    def cancel_context(self) -> None:
        self.context.cancel()

    def wait(self, seconds: Union[int, float]) -> None:
        if self.context.time_remaining() > seconds:
            self._await_sync(seconds)
        else:
            logger.warning('Remaining time of context less than wait time. Cache check starts now.')

    def _await_sync(self, timeout: Union[int, float]) -> None:
        try:
            self.time_lock.acquire()
            self.time_condition.wait(timeout)
        finally:
            self.time_lock.release()
