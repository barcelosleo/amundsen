# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import requests
from collections import namedtuple
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Union
from pyhocon import ConfigTree

from databuilder.extractor.base_extractor import Extractor

LOGGER = logging.getLogger(__name__)


class BaseMetabaseExtractor(Extractor):
    METABASE_URL_KEY = "base_url"
    API_USER_KEY = "api_user"
    API_PASSWORD_KEY = "api_password"

    METABASE_SESSION_TOKEN = None

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf
        self.base_url = conf.get_string(self.METABASE_URL_KEY, None)
        self.api_user = conf.get_string(self.API_USER_KEY, None)
        self.api_password = conf.get_string(self.API_PASSWORD_KEY, None)

        self._extract_iter: Iterator[Any] = iter([])

        self._metabase_login()

    def _metabase_login(self) -> None:
        request_body = {
            "username": self.api_user,
            "password": self.api_password,
        }
        headers = {"Content-Type": "application/json"}
        url = f"{self.base_url}/api/session"

        response = requests.api.post(url, json=request_body, headers=headers)

        if response.status_code != 200:
            LOGGER.error(
                f"Got HTTP response with status code {response.status_code}"
            )
            raise Exception("Unable to login to Metabase")

        response_json = response.json()

        if "error" in response_json:
            for error in response_json["error"]:
                LOGGER.error(f"{error} -> {response_json['error'][error]}")

            raise Exception("Unable to login to Metabase")

        self.METABASE_SESSION_TOKEN = response_json["id"]

    def _get_metabase_database(self, database_id: int) -> Dict:
        response = self._metabase_get(f"database/{database_id}")
        return response.json()

    def _get_metabase_table(self, table_id: int) -> Dict:
        response = self._metabase_get(f"table/{table_id}").json()
        response["database_data"] = self._get_metabase_database(
            response["db_id"]
        )
        return response

    def _metabase_get(
        self, endpoint, headers: Union[None, Dict] = None
    ) -> requests.Response:
        _headers = (
            headers
            if headers
            else {"X-Metabase-Session": self.METABASE_SESSION_TOKEN}
        )
        url = f"{self.base_url}/api/{endpoint}"

        return requests.api.get(url, headers=_headers)

    def extract(self) -> Any:
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return "extractor.base_metabase_metadata_extractor"
