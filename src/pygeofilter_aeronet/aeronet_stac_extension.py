# Copyright 2025 Terradue
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from typing import Any, Dict, Literal, Optional

import pystac
from pystac.extensions.base import ExtensionManagementMixin, PropertiesExtension
from pystac.utils import get_required

# Constants
AERONET_SCHEMA_URI: str = "https://raw.githubusercontent.com/Terradue/aeronet-stac-extension/refs/heads/main/json-schema/schema.json"

AERONET_PREFIX: str = "aeronet:"

SITE_NAME_PROP = AERONET_PREFIX + "site_name"
LAND_USE_TYPE_PROP = AERONET_PREFIX + "land_use_type"
L10_PROP = AERONET_PREFIX + "L10"
L15_PROP = AERONET_PREFIX + "L15"
L20_PROP = AERONET_PREFIX + "L20"
MOON_L20_PROP = AERONET_PREFIX + "moon_L20"

class AeronetExtension(
    PropertiesExtension,
    ExtensionManagementMixin[pystac.Item],
):
    """A concrete implementation of the AERONET extension on a pystac.Item.

    All AERONET properties are defined as required in the JSON Schema:

      * aeronet:site_name   -> str
      * aeronet:land_use_type -> str
      * aeronet:L10         -> int
      * aeronet:L15         -> int
      * aeronet:L20         -> int
      * aeronet:moon_L20    -> int
    """

    # this name is only needed if you ever want to wire it into Item.ext
    name: Literal["aeronet"] = "aeronet"

    item: pystac.Item
    properties: Dict[str, Any]

    def __init__(self, item: pystac.Item) -> None:
        self.item = item
        self.properties = item.properties

    # ---- Required by PropertiesExtension ----
    @classmethod
    def get_schema_uri(cls) -> str:
        return AERONET_SCHEMA_URI

    # ---- Main helper to attach to an Item ----
    @classmethod
    def ext(
        cls,
        obj: pystac.Item,
        add_if_missing: bool = False,
    ) -> "AeronetExtension":
        if isinstance(obj, pystac.Item):
            # ensures schema URI is in item.stac_extensions
            cls.ensure_has_extension(obj, add_if_missing)
            return cls(obj)
        else:
            raise pystac.ExtensionTypeError(
                f"AeronetExtension does not apply to type '{type(obj).__name__}'"
            )

    # Convenience alias, like in the tutorial
    @classmethod
    def from_item(
        cls,
        item: pystac.Item,
        add_if_missing: bool = False,
    ) -> "AeronetExtension":
        return cls.ext(item, add_if_missing=add_if_missing)

    # ---- Required properties (getters / setters) ----

    @property
    def site_name(self) -> str:
        return get_required(
            self._get_property(SITE_NAME_PROP, str),
            self,
            SITE_NAME_PROP,
        )

    @site_name.setter
    def site_name(self, v: str) -> None:
        self._set_property(SITE_NAME_PROP, v, pop_if_none=False)

    @property
    def land_use_type(self) -> str:
        return get_required(
            self._get_property(LAND_USE_TYPE_PROP, str),
            self,
            LAND_USE_TYPE_PROP,
        )

    @land_use_type.setter
    def land_use_type(self, v: str) -> None:
        self._set_property(LAND_USE_TYPE_PROP, v, pop_if_none=False)

    @property
    def L10(self) -> int:
        return get_required(
            self._get_property(L10_PROP, int),
            self,
            L10_PROP,
        )

    @L10.setter
    def L10(self, v: int) -> None:
        self._set_property(L10_PROP, v, pop_if_none=False)

    @property
    def L15(self) -> int:
        return get_required(
            self._get_property(L15_PROP, int),
            self,
            L15_PROP,
        )

    @L15.setter
    def L15(self, v: int) -> None:
        self._set_property(L15_PROP, v, pop_if_none=False)

    @property
    def L20(self) -> int:
        return get_required(
            self._get_property(L20_PROP, int),
            self,
            L20_PROP,
        )

    @L20.setter
    def L20(self, v: int) -> None:
        self._set_property(L20_PROP, v, pop_if_none=False)

    @property
    def moon_L20(self) -> int:
        return get_required(
            self._get_property(MOON_L20_PROP, int),
            self,
            MOON_L20_PROP,
        )

    @moon_L20.setter
    def moon_L20(self, v: int) -> None:
        self._set_property(MOON_L20_PROP, v, pop_if_none=False)
