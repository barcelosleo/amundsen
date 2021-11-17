# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, Dict, Union, Iterator

from pyhocon import ConfigTree

from databuilder.extractor.base_metabase_extractor import BaseMetabaseExtractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata

LOGGER = logging.getLogger(__name__)


class MetabaseCardMetadataExtractor(BaseMetabaseExtractor):
    """
    An extractor extracts record
    """

    def init(self, conf: ConfigTree) -> None:
        super().init(conf)

        self._extract_iter: Union[None, Iterator] = None

    def _get_cards(self) -> Dict:
        response = self._metabase_get("card")

        response_json = response.json()

        if len(response_json) > 0:
            for card in response_json:
                card["table_data"] = self._get_metabase_table(card["table_id"])

        return response_json

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        cards_metadata = self._get_cards()

        if not cards_metadata:
            return None

        for card in cards_metadata:
            fields = []
            for field in card["result_metadata"]:
                fields.append(
                    ColumnMetadata(
                        name=field["display_name"],
                        description=field["description"],
                        col_type=field["effective_type"],
                        sort_order=field["id"],
                    )
                )

            yield TableMetadata(
                database=card["table_data"]["database_data"]["name"],
                cluster="",
                schema=card["table_data"]["schema"],
                name=card["name"],
                description=card["description"],
                columns=fields,
                origin_table=card["table_data"]["name"],
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
