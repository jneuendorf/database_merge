from collections import OrderedDict

from .MergeStrategy import MergeStrategy

class RowsDict():
    """This class is a wrapper around a ordered dictionary `rows`.
    Each instance is associated with a table with 'self.table_name'.
    Rows are merged according to 'self.strategy'.
    'self.rows' is a dictionary mapping row hashes to tuples
    consisting of a row and a flag indicating if the row is from the source
    or from the target table.
    {row_hash: (row, is_from_source)}
    """

    def __init__(self, table_name: str, strategy: MergeStrategy) -> None:
        self.table_name = table_name
        self.strategy = strategy
        self.rows = OrderedDict()

    def get(self, key):
        return self.rows.get(key)

    def put(self, row_hash, row, origin):
        # if len(row) > 4 and row[4] == "B4f":
        #     # TODO: this is weird:
        #     # row[3] == "Not Mustermann" from db_merge_test2, origin == "source"
        #     import pudb; pudb.set_trace()
        if row_hash in self.rows:
            print("using a strategy to choose a row!")
            # import pudb; pudb.set_trace()
            chosen_row = self.strategy.choose_row(
                (row, origin),
                self.rows[row_hash],
            )
            self.rows[row_hash] = (chosen_row, origin)
        else:
            self.rows[row_hash] = (row, origin)

    def get_rows(self):
        return (row for row_hash, (row, origin) in self.rows.items())

    def __str__(self):
        rows = list(self.rows.values())
        return f"RowsDict(strategy={self.strategy.__class__.__name__}, rows={rows})"

    def __iter__(self):
        return self.rows
