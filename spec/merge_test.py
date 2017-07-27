import os
from typing import Dict, List, Union
import unittest

from sqlalchemy import create_engine, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
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
        class User(Base): # type: ignore
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            name = Column(String)
            password = Column(String)
            orders = relationship("Order")
        self.User = User

        class Order(Base): # type: ignore
            __tablename__ = "orders"
            id = Column(Integer, primary_key=True)
            # Using string instead of numeric to avoid sqlalchemy's warning.
            total = Column(String)
            user_id = Column(Integer, ForeignKey("users.id"))
            user = relationship("User", back_populates="orders")
        self.Order = Order

        engine = create_engine(self.DB_URL)
        engine2 = create_engine(self.DB2_URL)
        Base.metadata.create_all(bind=engine)
        Base.metadata.create_all(bind=engine2)

        self.db = db_helpers.get_reflected_db(self.DB_URL)
        self.db2 = db_helpers.get_reflected_db(self.DB2_URL)

        # DATA SETUP
        db_helpers.insert_rows(self.db, db_helpers.get_table(self.db, "users"), [
            (1, "testuser1", "pw1"),
            (2, "testuser2 with more data", "pw2"),
            (3, "testuser3", "pw3"),
            (4, "testuser4", "pw4"),
        ])
        db_helpers.insert_rows(self.db, db_helpers.get_table(self.db, "orders"), [
            (1, "30.12", 1),
            (2, "12.39", 2),
            (3, "42.00", 3),
            (4, "43.00", 4),
        ])
        db_helpers.insert_rows(self.db2, db_helpers.get_table(self.db2, "users"), [
            (2, "testuser2", "pw21"),
            (3, "testuser3 with more data", "pw3"),
            (7, "testuser4", "pw4"),
        ])
        db_helpers.insert_rows(self.db2, db_helpers.get_table(self.db2, "orders"), [
            (2, "13.37", 2),
            (5, "51.10", 3),
            (6, "1.18", 7),
        ])

    def tearDown(self):
        os.remove(self.SQLITE_FILE)
        os.remove(self.SQLITE_FILE2)

    @classmethod
    def get_input_kwargs(cls) -> Dict[str, Union[str, List[str]]]:
        return {
            "db_urls": [cls.DB_URL, cls.DB2_URL],
            "target_db_url": cls.DB_URL_TARGET,
        }


    ###########################################################################
    # TESTS
    def test_source_merge(self):
        input_data = Input(**self.get_input_kwargs(), strategy=strategies.SourceMergeStrategy())
        target = merge(input_data)

        def strip_ids(rows):
            return [row[1:] for row in rows]

        users = db_helpers.get_rows(target, "users")
        # compare rows without IDs (order doesn't matter)
        self.assertEqual(
            set(strip_ids(users)),
            set(strip_ids([
                (1, "testuser1", "pw1"),
                (2, "testuser2 with more data", "pw2"),
                (3, "testuser3", "pw3"),
                (4, "testuser4", "pw4"),
                (2, "testuser2", "pw21"),
                (3, "testuser3 with more data", "pw3"),
            ]))
        )
        # make sure IDs are unique
        self.assertEqual(
            len(users),
            len(set([row[0] for row in users]))
        )


        users = target.session.query(self.User).all()
        # orders = target.session.query(self.Order).all()

        totals_by_user = {
            ("testuser1", "pw1"): {"30.12"},
            ("testuser2 with more data", "pw2"): {"12.39"},
            ("testuser3", "pw3"): {"42"},
            ("testuser2", "pw21"): {"13.37"},
            ("testuser3 with more data", "pw3"): {"51.10"},
            # there should be both orders because the user exists in both databases
            ("testuser4", "pw4"): {"43", "1.18"},
        }

        for user in users:
            print((user.name, user.password))
            print("expect:", totals_by_user[(user.name, user.password)])
            print("got:", {float(order.total) for order in user.orders})
            print("")
            self.assertEqual(
                {float(order.total) for order in user.orders},
                totals_by_user[(user.name, user.password)]
            )
