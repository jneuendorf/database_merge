from .MergeStrategy import MergeStrategy


class TargetMergeStrategy(MergeStrategy):
    """Does the opposite of what the SourceMergeStrategy does."""

    def _choose_row(self, *rows_data) -> tuple:
        for row, source in rows_data:
            if source == "target":
                return row
        raise ValueError("Found no row with source 'target'")
