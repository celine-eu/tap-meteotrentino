"""REST client handling, including MeteoTrentinoStream base class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any
from xml.etree import ElementTree

from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context

MT_NS = "http://www.meteotrentino.it/"
MT_NSMAP = {"mt": MT_NS}


class MeteoTrentinoStream(RESTStream):
    """MeteoTrentino base JSON REST stream."""

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None  # no pagination

    @property
    @override
    def url_base(self) -> str:
        return ""

    @override
    def get_new_paginator(self) -> None:
        return None

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        return {}


class MeteoTrentinoXMLStream(RESTStream):
    """MeteoTrentino base XML REST stream."""

    @property
    @override
    def url_base(self) -> str:
        return ""

    @override
    def get_new_paginator(self) -> None:
        return None

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        return {}

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        root = ElementTree.fromstring(response.content)
        yield from self._parse_xml(root)

    def _parse_xml(self, root: ElementTree.Element) -> Iterable[dict]:
        raise NotImplementedError
