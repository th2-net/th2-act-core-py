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
from typing import Callable, Dict

import grpc

logger = logging.getLogger()


class GRPCServer:
    """Act server using ActHandler class instances described in act implementation.

    Args:
        server (grpc.Server): Server of gRPC router from CommonFactory.
        handlers (Dict[ActHandler, add_servicer_to_server()]): a dict with ActHandler class instance as key and
            add_servicer_to_server() function as value. This parameter can be created by Act.load_handlers() method.
    """

    def __init__(self, server: grpc.Server, handlers: Dict[Callable, Callable]) -> None:
        self.server = server
        self.handlers = handlers

    def start(self) -> None:
        """Starts the server. Returns None"""

        if self.handlers:
            for handler in self.handlers:
                add_servicer_to_server_function = self.handlers[handler]
                add_servicer_to_server_function(handler, self.server)

            self.server.start()
            logger.info('GRPC Server started')
            logger.info(f'Services: {[service.service_name() for service in self.server._state.generic_handlers]}')

            self.server.wait_for_termination()
        else:
            logger.error('Cannot start GRPC server: no handlers uploaded')

    def stop(self) -> None:
        """Stops the server. Returns None"""

        self.server.stop(None)
        logger.info('GRPC Server stopped')
