import logging
import re

from sqlalchemy import Column


# TODO: check for constraints/keys; those get lost when automapping...
def columns_equal(col1: Column, col2: Column):
    return column_types_equal(col1.type, col2.type) and col1.name == col2.name


# TODO: make this more robust: compare certain attributes depending on the object's type
def column_types_equal(col_type1, col_type2):
    str_type1 = str(col_type1)
    str_type2 = str(col_type2)
    if str_type1 == str_type2:
        return True
    # Ignore varchar's charset because for some reason it gets lost when automapping...
    # VARCHAR(100) CHARACTER SET utf8 COLLATE utf8_unicode_ci
    # VARCHAR(100)                    COLLATE utf8_unicode_ci
    logging.info(
        f"column types did not match ({str_type1} != {str_type2}). "
        + "ignoring /CHARACTER SET .*? COLLATE/i"
    )
    regex = re.compile(r"CHARACTER SET .*? COLLATE", re.IGNORECASE)
    str_type1 = regex.sub("COLLATE", str_type1)
    str_type2 = regex.sub("COLLATE", str_type2)
    return str_type1 == str_type2
