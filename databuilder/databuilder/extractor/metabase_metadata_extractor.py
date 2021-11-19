# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, Dict, List, Union, Iterator

from pyhocon import ConfigTree

from databuilder.extractor.base_metabase_extractor import BaseMetabaseExtractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata

LOGGER = logging.getLogger(__name__)


class MetabaseMetadataExtractor(BaseMetabaseExtractor):
    """
    An extractor that extracts metadata from every database configured as
    source for metabase
    """

    def init(self, conf: ConfigTree) -> None:
        super().init(conf)

        self._extract_iter: Union[None, Iterator] = None

    def _get_databases(self) -> Dict:
        response = self._metabase_get("database")
        response_json = response.json()

        # Backwards compatibility
        if "data" in response_json:
            response_json = response_json["data"]

        LOGGER.info(f"Found {len(response_json)} databases...")

        return response_json

    def _get_metabase_databases_metadata(self) -> List:
        databases = self._get_databases()

        databases_metadata = []

        for database in databases:
            LOGGER.info(f"Extracting metadata from \"{database['name']}\"...")

            response = self._metabase_get(
                f"database/{database['id']}/metadata"
            )
            databases_metadata.append(response.json())

        return databases_metadata

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        databases_metadata = self._get_metabase_databases_metadata()

        for database_metadata in databases_metadata:
            for table in database_metadata["tables"]:
                fields = []
                for field in table["fields"]:
                    fields.append(
                        ColumnMetadata(
                            name=field["name"],
                            description=field["description"],
                            col_type=field["effective_type"],
                            sort_order=field["position"],
                        )
                    )

                yield TableMetadata(
                    database=database_metadata["name"],
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
