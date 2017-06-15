from collections import OrderedDict


class RowsDict():
    """This class is a wrapper around a dictionary `rows`.
    self.rows is a dictionary mapping row hashes to tuples
    consisting of a row and a flag indicating if the row is from the source
    or from the target table.
    {row_hash: (row, is_from_source)}
    """

    def __init__(self, strategy):
        self.strategy = strategy
        self.rows = OrderedDict()

    def get(self, key):
        return self.rows.get(key)

    def put(self, row_hash, row, origin):
        if row_hash in self.rows:
            self.rows[row_hash] = self.strategy.choose_row(
                (row, origin),
                self.rows[row_hash],
            )
        else:
            self.rows[row_hash] = (row, origin)

    def __str__(self):
        rows = list(self.rows.values())
        return f"RowsDict(strategy={self.strategy.__class__.__name__}, rows={rows})"
