import unittest

import spec




# assertEqual(a, b)
# assertNotEqual(a, b)
# assertTrue(x)
# assertFalse(x)
# assertIs(a, b)
# assertIsNot(a, b)
# assertIsNone(x)
# assertIsNotNone(x)
# assertIn(a, b)
# assertNotIn(a, b)
# assertIsInstance(a, b)
# assertNotIsInstance(a, b)

class MockTest(unittest.TestCase):

    def setUp(self):
        self.tables = spec.create_test_data([
            {
                "name": "MyTable",
                "columns": [
                    {"name": "col1", "data_type": spec.DataType.INT},
                    {"name": "col2", "data_type": spec.DataType.STRING},
                ],
                "rows": [
                    [1, "row1"],
                    [2, "row2"],
                ],
            }
        ])
        if len(self.tables) > 0:
            self.table = self.tables[0]
        else:
            self.table = None



    def test_mocked_orm(self):
        self.assertEqual(self.table.name, "MyTable")
        self.assertEqual(
            [column.type for column in self.table.columns],
            [spec.DataType.INT, spec.DataType.STRING]
        )
