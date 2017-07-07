import unittest

from strategies import MergeStrategy, RowsDict


class PseudoMergeStrategy(MergeStrategy):

    def _choose_row(self, *rows_data) -> tuple:
        return rows_data[0][0]


class RowsDictTest(unittest.TestCase):

    @classmethod
    def get_test_rows(cls):
        return [
            {
                "row_hash": 0,
                "row": ("source0"),
                "origin": "source"
            },
            {
                "row_hash": 0,
                "row": ("source1"),
                "origin": "source"
            },
            {
                "row_hash": 1,
                "row": ("source2"),
                "origin": "source"
            },
            {
                "row_hash": 2,
                "row": ("target0"),
                "origin": "target"
            },
        ]

    def setUp(self):
        self.rows_dict = RowsDict("pseudo_table", PseudoMergeStrategy())

    ###########################################################################
    # TESTS
    def test_put(self):
        for kwargs in self.get_test_rows():
            self.rows_dict.put(**kwargs)
        self.assertEqual(
            list(self.rows_dict.rows.items()),
            [
                (0, (("source0"), "source")),
                (1, (("source2"), "source")),
                (2, (("target0"), "target")),
            ]
        )

    def test_get(self):
        self.test_put()
        self.assertEqual(self.rows_dict.get(0), (("source0"), "source"))
        self.assertEqual(self.rows_dict.get(1), (("source2"), "source"))
        self.assertEqual(self.rows_dict.get(2), (("target0"), "target"))

    def test_get_rows(self):
        self.test_put()
        self.assertEqual(
            list(self.rows_dict.get_rows()),
            [("source0"), ("source2"), ("target0")]
        )

    def test_iter(self):
        self.test_put()
        i = 0
        for row_hash in self.rows_dict:
            self.assertEqual(row_hash, i)
            i += 1

    def test_str(self):
        self.assertEqual(
            str(self.rows_dict),
            "RowsDict(strategy=PseudoMergeStrategy, rows=[])"
        )
