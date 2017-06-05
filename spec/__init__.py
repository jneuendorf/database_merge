import enum

from .mocked_orm import Table, Column, Row, create_test_data


class DataType(enum.Enum):
    INT = 1
    STRING = 2
    DATE = 3
    DOUBLE = 4
