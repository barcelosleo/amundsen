# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import unittest
from typing import Any, Dict

from pyhocon import ConfigFactory

from databuilder import Scoped
from databuilder.extractor.metabase_metadata_extractor import (
    MetabaseMetadataExtractor,
)
from databuilder.extractor.metabase_card_metadata_extractor import (
    MetabaseCardMetadataExtractor,
)
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata


class TestAthenaMetadataExtractor(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(level=logging.INFO)

        config_dict = {
            f"extractor.metabase_metadata_extractor.{MetabaseCardMetadataExtractor.METABASE_URL_KEY}": "https://metabase.indicium.tech",
            f"extractor.metabase_metadata_extractor.{MetabaseCardMetadataExtractor.API_USER_KEY}": "leonardo.machado@indicium.tech",
            f"extractor.metabase_metadata_extractor.{MetabaseCardMetadataExtractor.API_PASSWORD_KEY}": "l20952509@Indicium",
        }
        self.conf = ConfigFactory.from_dict(config_dict)

    def test_extractor(self) -> None:
        extractor = MetabaseCardMetadataExtractor()
        extractor.init(
            Scoped.get_scoped_conf(conf=self.conf, scope=extractor.get_scope())
        )

        while True:
            extracted = extractor.extract()

            if not extracted:
                break

            print(extracted)

        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
