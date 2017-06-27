import os
import unittest

from sqlalchemy import create_engine, Column, ForeignKey, Integer, Numeric, String, Table
from sqlalchemy.ext.declarative import declarative_base

import db_helpers


class DbHelpersTest(unittest.TestCase):

    SQLITE_FILE = "test.db"
    # 'test.db' is a relative path
    DB_URL = f"sqlite:///{SQLITE_FILE}"

    # @classmethod
    # def setUpClass(cls):
    #     pass
    #
    # @classmethod
    # def tearDownClass(cls):
    #     pass

    def setUp(self):
        Base = declarative_base()
        class User(Base):
            __tablename__ = "users"

            id = Column(Integer, primary_key=True)
            name = Column(String)
            password = Column(String)

            def __repr__(self):
                return f"<User(name='{self.name}', password='{self.password}')>"

        class Order(Base):
            __tablename__ = "orders"

            id = Column(Integer, primary_key=True)
            items = Column(String)
            total = Column(Numeric)
            user_id = Column(Integer, ForeignKey("users.id"))

        engine = create_engine(self.DB_URL)
        Base.metadata.create_all(engine)

        self.db = db_helpers.get_reflected_db(self.DB_URL)
        # shortcut
        self.tables = self.db.base.metadata.tables

    def tearDown(self):
        self.db.base.metadata.drop_all(bind=self.db.engine)
        os.remove(self.SQLITE_FILE)

    def test_setup(self):
        self.assertEqual(
            sorted(list(self.tables.keys())),
            ["orders", "users"]
        )

    def test_get_table(self):
        user_table = db_helpers.get_table(self.db, "users")
        order_table = db_helpers.get_table(self.db, "orders")

        self.assertIsInstance(user_table, Table)
        self.assertEqual(user_table.name, "users")
        self.assertIsInstance(order_table, Table)
        self.assertEqual(order_table.name, "orders")
