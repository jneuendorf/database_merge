from collections import OrderedDict
from typing import Iterable, Tuple, Union

from .MergeStrategy import MergeStrategy


class RowsDict():
    """This class is a wrapper around a ordered dictionary `rows`.
    Each instance is associated with a table with 'self.table_name'.
    Rows are merged according to 'self.strategy'.
    """

    def __init__(self, table_name: str, strategy: MergeStrategy) -> None:
        self.table_name = table_name
        self.strategy = strategy
        # mapping: row hash -> (chosen_row: list, origin: str, primary_keys: set)
        self.rows: OrderedDict = OrderedDict()

    def get(self, key) -> Union[Tuple[tuple, str], None]:
        return self.rows.get(key, None)

    # TODO: use db_helper for finding primary key
    # ASSUMPTION: IDs as primary keys in 1st column
    def put(self, row_hash: int, row: tuple, origin: str) -> None:
        if row_hash in self.rows:
            chosen_row = self.strategy.choose_row(
                self.rows[row_hash][0:2],
                (row, origin),
            )
            prev_row, _, _ = self.rows[row_hash]
            if chosen_row[0] != row[0] and chosen_row[0] != prev_row[0]:
                raise ValueError(
                    "The row returned by a merge strategy must have the primary of either row."
                )
            self.rows[row_hash] = (
                chosen_row,
                origin,
                # Keep primary keys of both given rows.
                # The 'chosen_row' should always have the primary key of either given row.
                frozenset((prev_row[0], row[0]))
            )
        else:
            self.rows[row_hash] = (list(row), origin, frozenset((row[0], )))

    def get_rows(self) -> Iterable[list]:
        return (row for row_hash, (row, origin, primary_keys) in self.rows.items())

    def get_rows_with_pks(self) -> Iterable[Tuple[list, frozenset]]:
        return ((row, primary_keys) for row_hash, (row, origin, primary_keys) in self.rows.items())

    def __str__(self) -> str:
        rows = list(self.rows.values())
        return f"RowsDict(strategy={self.strategy.__class__.__name__}, rows={rows})"

    def __iter__(self):
        return iter(self.rows)
