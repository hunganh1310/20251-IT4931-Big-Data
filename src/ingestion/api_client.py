from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import requests

from ..common import logger


class APIClientError(RuntimeError):
    pass


@dataclass
class AirQualityAPIClient:

    base_url: str
    token: Optional[str] = None
    api_key: Optional[str] = None
    session: Optional[requests.Session] = None
    timeout: int = 10

    def __post_init__(self) -> None:
        self._session = self.session or requests.Session()
        if self.api_key:
            self._session.headers.update({"X-API-Key": self.api_key})

    def fetch(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        url = f"{self.base_url}/{endpoint}"
        request_params = params or {}

        if self.token:
            request_params["token"] = self.token

        logger.info("Fetching from %s with params %s", url, request_params)
        response = self._session.get(url, params=request_params, timeout=self.timeout)

        if response.status_code != 200:
            raise APIClientError(
                f"API returned status {response.status_code} for {url}"
            )

        return response.json()

    def close(self) -> None:
        if self.session is None and self._session:
            self._session.close()

    def __enter__(self) -> "AirQualityAPIClient":
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        self.close()
