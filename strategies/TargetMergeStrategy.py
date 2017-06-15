from . import MergeStrategy


class TargetMergeStrategy(MergeStrategy):
    """Does the opposite of what the SourceMergeStrategy does."""

    def choose_row(self, *rows_data):
        for row, source in rows_data:
            if source == "target":
                return row
        raise ValueError("Found no row with source 'target'")
