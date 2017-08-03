import logging


class MergeStrategy():

    # `rows_data` is a list of tuples, each tuple being (row, origin)
    def choose_row(self, *rows_data) -> list:
        row = self._choose_row(*rows_data)
        logging.info(
            f"chose {str(row)} "
            f"from {str(rows_data)} "
            f" using {self.__class__.__name__}"
        )
        return list(row)

    def _choose_row(self, *rows_data) -> tuple:
        raise NotImplementedError("Must implement 'choose_row'")
