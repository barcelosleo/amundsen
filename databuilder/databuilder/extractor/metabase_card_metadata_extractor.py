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
    An extractor that extracts card metadata from metabase
    """

    def init(self, conf: ConfigTree) -> None:
        super().init(conf)

        self._extract_iter: Union[None, Iterator] = None

    def _get_cards(self) -> Dict:
        response = self._metabase_get("card")

        response_json = response.json()

        return response_json

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        cards = self._get_cards()

        total_cards = len(cards)
        loaded = 1

        if total_cards > 0:
            LOGGER.info(f"Found {total_cards} Cards...")
            for card in cards:
                LOGGER.info(
                    f"Extracting metadata from the card {loaded} of {total_cards} \"{card['name']}\"..."
                )
                card["table_data"] = (
                    self._get_metabase_table(card["table_id"])
                    if card["table_id"]
                    else None
                )
                card["database_data"] = (
                    self._get_metabase_database(card["database_id"])
                    if card["database_id"]
                    else None
                )

                fields = []
                for field in card["result_metadata"]:

                    fields.append(
                        ColumnMetadata(
                            name=field["display_name"],
                            description=field["description"]
                            if "description" in field
                            else "",
                            col_type=field["base_type"],
                            sort_order=field["id"] if "id" in field else 0,
                        )
                    )

                database_name = ""
                schema_name = ""
                origin_table_name = ""

                if card["database_data"]:
                    database_name = card["database_data"]["name"]

                if card["table_data"]:
                    schema_name = card["table_data"]["schema"]
                    origin_table_name = card["table_data"]["name"]

                yield TableMetadata(
                    database=database_name,
                    cluster="",
                    schema=schema_name,
                    name=card["name"],
                    description=card["description"],
                    columns=fields,
                    origin_table=origin_table_name,
                )

                loaded += 1

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
