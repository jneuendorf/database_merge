import os
import unittest

from sqlalchemy import create_engine, Column, ForeignKey, Integer, Numeric, String
from sqlalchemy.ext.declarative import declarative_base

import db_helpers
from Input import Input
from merge import merge
import strategies

class MergeTest(unittest.TestCase):

    # database reflection apparently does not work for in-memory sqlite dbs
    SQLITE_FILE = "test.db"
    SQLITE_FILE2 = "test2.db"
    SQLITE_FILE_TARGET = "test_target.db"
    DB_URL = f"sqlite:///{SQLITE_FILE}"
    DB2_URL = f"sqlite:///{SQLITE_FILE2}"
    DB_URL_TARGET = f"sqlite:///{SQLITE_FILE_TARGET}"

    def setUp(self):
        Base = declarative_base()
        # pylint: disable=unused-variable
        class User(Base): # type: ignore
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            name = Column(String)
            password = Column(String)

        # pylint: disable=unused-variable
        class Order(Base): # type: ignore
            __tablename__ = "orders"
            id = Column(Integer, primary_key=True)
            total = Column(Numeric)
            user_id = Column(Integer, ForeignKey("users.id"))

        engine = create_engine(self.DB_URL)
        engine2 = create_engine(self.DB2_URL)
        Base.metadata.create_all(bind=engine)
        Base.metadata.create_all(bind=engine2)

        self.db = db_helpers.get_reflected_db(self.DB_URL)
        self.db2 = db_helpers.get_reflected_db(self.DB2_URL)

        # DATA SETUP
        db_helpers.insert_rows(self.db, db_helpers.get_table(self.db, "users"), [
            (1, "testuser1", "pw1"),
            (2, "testuser2 with data that should be overridden", "pw2"),
            (3, "testuser3", "pw3"),
        ])
        db_helpers.insert_rows(self.db, db_helpers.get_table(self.db, "orders"), [
            (1, 30.12, 1),
            (2, 12.39, 2),
            (3, 42.00, 3),
        ])
        db_helpers.insert_rows(self.db2, db_helpers.get_table(self.db2, "users"), [
            (2, "testuser2", "pw2"),
            (3, "testuser3 with more data", "pw3"),
        ])
        db_helpers.insert_rows(self.db2, db_helpers.get_table(self.db2, "orders"), [
            (2, 13.37, 3),
            (5, 51.10, 3),
        ])

    def tearDown(self):
        os.remove(self.SQLITE_FILE)
        os.remove(self.SQLITE_FILE2)

    @classmethod
    def get_input_kwargs(cls):
        return {
            "db_urls": [cls.DB_URL, cls.DB2_URL],
            "target_db_url": cls.DB_URL_TARGET,
        }


    ###########################################################################
    # TESTS
    def test_merge(self):
        input_data = Input(**self.get_input_kwargs(), strategy=strategies.SourceMergeStrategy())
        merge(input_data)
        # self.assertEqual(merge.columns, self.tables[0].columns)
