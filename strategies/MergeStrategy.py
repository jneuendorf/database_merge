import logging


class MergeStrategy():

    def choose_row(self, *rows_data) -> tuple:
        row = self._choose_row(*rows_data)
        logging.info(
            f"chose {str(row)} "
            f"from {str(rows_data)} "
            f" using {self.__class__.__name__}"
        )
        return row

    def _choose_row(self, *rows_data) -> tuple:
        raise NotImplementedError("Must implement 'choose_row'")
