import os
import unittest

from sqlalchemy import create_engine, Column, ForeignKey, Integer, Numeric, String, Table
from sqlalchemy.ext.declarative import declarative_base

import db_helpers


class DbHelpersTest(unittest.TestCase):

    SQLITE_FILE = "test.db"
    # 'test.db' is a relative path
    DB_URL = f"sqlite:///{SQLITE_FILE}"
    DB2_URL = f"sqlite://"

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
            items = Column(String)
            total = Column(Numeric)
            user_id = Column(Integer, ForeignKey("users.id"))

        engine = create_engine(self.DB_URL)
        Base.metadata.create_all(engine)

        self.db = db_helpers.get_reflected_db(self.DB_URL)
        self.db2 = db_helpers.get_reflected_db(self.DB2_URL)
        # shortcut
        self.tables = self.db.base.metadata.tables

    def tearDown(self):
        # self.db.base.metadata.drop_all(bind=self.db.engine)
        os.remove(self.SQLITE_FILE)

    @staticmethod
    def get_test_data():
        return [
            (1, "testuser1", "pw1"),
            (2, "testuser2", "pw2"),
            (3, "testuser3", "pw3"),
        ]

    ###########################################################################
    # TESTS
    def test_setup(self):
        self.assertEqual(
            sorted(list(self.tables.keys())),
            ["orders", "users"]
        )

    def test_get_reflected_db(self):
        # some if this has already been implicitly tested in setUp and test_setup
        def invalid_db_url():
            return db_helpers.get_reflected_db("asdf")
        self.assertRaises(ValueError, invalid_db_url)

    def test_get_table(self):
        user_table = db_helpers.get_table(self.db, "users")
        order_table = db_helpers.get_table(self.db, "orders")

        self.assertIsInstance(user_table, Table)
        self.assertEqual(user_table.name, "users")
        self.assertIsInstance(order_table, Table)
        self.assertEqual(order_table.name, "orders")

    def test_insert_rows(self):
        db_helpers.insert_rows(
            self.db,
            db_helpers.get_table(self.db, "users"),
            DbHelpersTest.get_test_data()
        )
        queried_rows = self.db.session.query(self.tables["users"]).all()
        self.assertEqual(queried_rows, DbHelpersTest.get_test_data())

    def test_get_rows(self):
        self.test_insert_rows()
        self.assertEqual(db_helpers.get_rows(self.db, "users"), DbHelpersTest.get_test_data())

    def test_truncate_table(self):
        self.test_insert_rows()
        db_helpers.truncate_table(self.db, "users")
        self.assertEqual(
            self.db.session.query(self.tables["users"]).all(),
            []
        )

    def test_copy_table(self):
        self.test_insert_rows()
        db_helpers.copy_table(self.db, self.db2, "users")
        self.assertIn("users", self.db2.inspector.get_table_names())
        self.assertEqual(
            [column["name"] for column in self.db2.inspector.get_columns("users")],
            ["id", "name", "password"]
        )
        # no model because no 2nd reflection has taken place
        self.assertNotIn("users", self.db2.base.metadata)
        # re-reflecting does NOT work...
        # self.db2 = db_helpers.get_reflected_db(self.DB2_URL)
        # self.assertIn("users", self.db2.base.metadata)

    def test_table_structures_equal(self):
        self.assertTrue(db_helpers.table_structures_equal(
            self.tables["users"],
            self.tables["users"]
        ))
        self.assertFalse(db_helpers.table_structures_equal(
            self.tables["users"],
            self.tables["orders"]
        ))

    def test_find_referencing_tables(self):
        self.assertEqual(
            db_helpers.find_referencing_tables(self.db, "users"),
            [db_helpers.get_table(self.db, "orders")]
        )
        self.assertEqual(
            db_helpers.find_referencing_tables(self.db, "orders"),
            []
        )
