# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import requests
import json
from typing import Any, Dict, Union, Iterator

from pyhocon import ConfigTree

from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata

LOGGER = logging.getLogger(__name__)


class MetabaseMetadataExtractor(Extractor):
    """
    An extractor extracts record
    """

    METABASE_SESSION_TOKEN = None

    def init(self, conf: ConfigTree) -> None:
        self.conf = conf
        self.metabase_base_url = conf.get_string("metabase_base_url")
        self.metabase_database_name = conf.get_string("metabase_database_name")
        self.metabase_api_user = conf.get_string("metabase_api_user")
        self.metabase_api_password = conf.get_string("metabase_api_password")

        self._extract_iter: Union[None, Iterator] = None

    def _metabase_login(self) -> None:
        request_body = {
            "username": self.metabase_api_user,
            "password": self.metabase_api_password,
        }
        headers = {"Content-Type": "application/json"}
        url = f"{self.metabase_base_url}/api/session"

        try:
            response = requests.api.post(
                url, json=request_body, headers=headers
            )

            response_json = response.json()

            self.METABASE_SESSION_TOKEN = response_json["id"]
        except requests.RequestException:
            LOGGER.error("Couldn't connect to Metabase API")

    def _get_metabase_database_info(self, database_name: str) -> Dict:
        headers = {"X-Metabase-Session": self.METABASE_SESSION_TOKEN}
        url = f"{self.metabase_base_url}/api/database"
        try:
            response = requests.api.get(url, headers=headers)
            response_json = response.json()

            for database in response_json["data"]:
                if database["name"] == database_name:
                    return database

        except requests.RequestException:
            LOGGER.error("Couldn't get database list from Metabase API")
            return None

        LOGGER.error(f'The database "{database_name}" was not found')

        return None

    def _get_metabase_database_metadata(self, database_name: str) -> Dict:
        headers = {"X-Metabase-Session": self.METABASE_SESSION_TOKEN}
        database_data = self._get_metabase_database_info(database_name)

        url = f"{self.metabase_base_url}/api/database/{database_data['id']}/metadata"

        try:
            response = requests.api.get(url, headers=headers)

            return response.json()
        except requests.RequestException:
            LOGGER.error("Couldn't get database metadata from Metabase API")

        return None

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        self._metabase_login()
        database_metadata = self._get_metabase_database_metadata(
            self.metabase_database_name
        )

        if not database_metadata:
            return None

        for table in database_metadata["tables"]:
            fields = []
            for field in table["fields"]:
                fields.append(
                    ColumnMetadata(
                        name=field["name"],
                        description=field["description"],
                        col_type=field["database_type"],
                        sort_order=field["position"],
                    )
                )

            yield TableMetadata(
                database=self.metabase_database_name,
                cluster="",
                schema=table["schema"],
                name=table["name"],
                description=table["description"],
                columns=fields,
            )

    def extract(self) -> Any:
        """
        :return: Provides a record or None if no more to extract
        """
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()

        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return "extractor.metabase_metadata_extractor"
