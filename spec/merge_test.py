import unittest

import spec
import strategies


class MergeTest(unittest.TestCase):

    def setUp(self):
        self.tables = spec.create_test_data([
            {
                "name": "source",
                "columns": [
                    {"name": "col1", "data_type": spec.DataType.INT},
                    {"name": "col2", "data_type": spec.DataType.STRING},
                ],
                "rows": [
                    [1, "row1"],
                    [2, "row2"],
                ],
            },
            {
                "name": "target",
                "columns": [
                    {"name": "col1", "data_type": spec.DataType.INT},
                    {"name": "col2", "data_type": spec.DataType.STRING},
                ],
                "rows": [
                    [1, "row1"],
                    [2, "row2"],
                ],
            },
        ])

    def test_merge_source(self):
        strategy = strategies.SourceMergeStrategy()
        merge = strategy.merge_tables(*self.tables)
        self.assertEqual(merge.columns, self.tables[0].columns)
