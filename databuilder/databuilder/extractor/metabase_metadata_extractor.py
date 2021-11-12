# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, Dict, Union, Iterator

from pyhocon import ConfigTree

from databuilder.extractor.base_metabase_extractor import BaseMetabaseExtractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata

LOGGER = logging.getLogger(__name__)


class MetabaseMetadataExtractor(BaseMetabaseExtractor):
    """
    An extractor extracts record
    """

    DATABASE_KEY = "database_name"

    METABASE_SESSION_TOKEN = None

    def init(self, conf: ConfigTree) -> None:
        super().init(conf)
        self.database_name = conf.get_string(self.DATABASE_KEY)

        self._extract_iter: Union[None, Iterator] = None

    def _get_metabase_database_info(self, database_name: str) -> Dict:
        response = self._metabase_get("database")
        response_json = response.json()

        for database in response_json["data"]:
            if database["name"] == database_name:
                return database

        raise Exception(f'The database "{database_name}" was not found')

    def _get_metabase_database_metadata(self, database_name: str) -> Dict:
        database_data = self._get_metabase_database_info(database_name)

        response = self._metabase_get(
            f"database/{database_data['id']}/metadata"
        )
        return response.json()

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        database_metadata = self._get_metabase_database_metadata(
            self.database_name
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
                database=self.database_name,
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
