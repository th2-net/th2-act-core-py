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

import importlib
import logging
from pathlib import Path
import traceback
from typing import Any, Callable, Dict, Optional

from th2_act_core.handler_attributes import HandlerAttributes
from th2_act_core.util.subscription_manager import SubscriptionManager
from th2_common.schema.message.message_router import MessageRouter

logger = logging.getLogger()


class Act:
    """Contains initialized ActHandler classes (uploaded from files) instances.

    Args:
        check1_service (Check1Service): Check1 service from gRPC router.
        message_router (MessageRouter): Message router from CommonFactory.
        event_router (MessageRouter): Event router from CommonFactory.
        handlers_package_relative_path (str, optional): A relative path to the package with the handlers.
            Defaults to '/handlers'.
    """

    def __init__(self,
                 check1_service: Any,
                 message_router: MessageRouter,
                 event_router: MessageRouter,
                 handlers_package_relative_path: str = 'handlers'):
        subscription_manager = SubscriptionManager()
        message_router.subscribe_all(subscription_manager)

        self._handler_attrs = HandlerAttributes(check1_connector=check1_service,
                                                message_router=message_router,
                                                event_router=event_router,
                                                subscription_manager=subscription_manager)

        self.handlers = self._load_handlers(handlers_package_relative_path)

    def _load_handlers(self, handlers_package_relative_path: str) -> Dict[Callable, Callable]:
        """Uploads ActHandler classes from files and initialize them. Returns a dict with ActHandler class instance
        as key and add_servicer_to_server() function as value. The dict can be passed as an argument when
        initializing an instance of the ActServer class.
        """

        handlers_servicers_dict = {}

        handlers_package = importlib.import_module(handlers_package_relative_path)
        handlers_package_name = handlers_package.__name__
        handlers_paths = Path(handlers_package.__path__[0]).glob('*.py')

        for handler_path in handlers_paths:
            try:
                handler_class = _get_handler_class_from_module(handlers_package_name, handler_path)

                if handler_class:
                    handler = handler_class(self._handler_attrs)
                    handlers_servicers_dict[handler] = _get_func_add_servicer_to_server(handler)
            except Exception as e:
                logger.error(f'Cannot upload handler with the path '
                             f'{handlers_package.__name__}.{handler_path.stem}: {e}'
                             f'\n{"".join(traceback.format_tb(e.__traceback__))}')

        logger.info(f'Handlers upload finished. Total: {len(handlers_servicers_dict)} handler(s)')

        return handlers_servicers_dict


def _get_handler_class_from_module(handlers_package_name: str, handler_path: Path) -> Optional[Callable]:
    handler_module = importlib.import_module(f'{handlers_package_name}.{handler_path.stem}')

    return getattr(handler_module, 'ActHandler', None)  # type: ignore


def _get_func_add_servicer_to_server(handler: Any) -> Any:
    servicer_class = handler.__class__.__base__
    servicer_module = importlib.import_module(servicer_class.__module__)

    return getattr(servicer_module, f'add_{servicer_class.__name__}_to_server')
