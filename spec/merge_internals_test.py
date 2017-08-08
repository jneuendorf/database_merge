# # import os
# # from typing import Dict, List, Union
# import unittest
#
# # from sqlalchemy import create_engine, Column, ForeignKey, Integer, String
# # from sqlalchemy.orm import relationship
# # from sqlalchemy.ext.declarative import declarative_base
#
# # import db_helpers
# from DbData import DbData
# from strategies import RowsDict, SourceMergeStrategy
# from merge import adjust_relationships
#
#
# class MergeInternalsTest(unittest.TestCase):
#
#     # database reflection apparently does not work for in-memory sqlite dbs
#     SQLITE_FILE = "test.db"
#     SQLITE_FILE2 = "test2.db"
#     SQLITE_FILE_TARGET = "test_target.db"
#     DB_URL = f"sqlite:///{SQLITE_FILE}"
#     DB2_URL = f"sqlite:///{SQLITE_FILE2}"
#     DB_URL_TARGET = f"sqlite:///{SQLITE_FILE_TARGET}"
#
#     def setUp(self):
#         Base = declarative_base()
#         class User(Base): # type: ignore
#             __tablename__ = "users"
#             id = Column(Integer, primary_key=True)
#             name = Column(String)
#             password = Column(String)
#             orders = relationship("Order")
#         self.User = User
#
#         class Order(Base): # type: ignore
#             __tablename__ = "orders"
#             id = Column(Integer, primary_key=True)
#             # Using string instead of numeric to avoid sqlalchemy's warning.
#             total = Column(String)
#             user_id = Column(Integer, ForeignKey("users.id"))
#             user = relationship("User", back_populates="orders")
#         self.Order = Order
#
#         engine = create_engine(self.DB_URL)
#         engine2 = create_engine(self.DB2_URL)
#         Base.metadata.create_all(bind=engine)
#         Base.metadata.create_all(bind=engine2)
#
#         self.db = db_helpers.get_reflected_db(self.DB_URL)
#         self.db2 = db_helpers.get_reflected_db(self.DB2_URL)
#
#         # DATA SETUP
#         db_helpers.insert_rows(self.db, db_helpers.get_table(self.db, "users"), [
#             (1, "testuser1", "pw1"),
#             (2, "testuser2 with more data", "pw2"),
#             (3, "testuser3", "pw3"),
#             (4, "testuser4", "pw4"),
#         ])
#         db_helpers.insert_rows(self.db, db_helpers.get_table(self.db, "orders"), [
#             (1, "30.12", 1),
#             (2, "12.39", 2),
#             (3, "42.00", 3),
#             (4, "43.00", 4),
#         ])
#         db_helpers.insert_rows(self.db2, db_helpers.get_table(self.db2, "users"), [
#             (2, "testuser2", "pw21"),
#             (3, "testuser3 with more data", "pw3"),
#             (7, "testuser4", "pw4"),
#         ])
#         db_helpers.insert_rows(self.db2, db_helpers.get_table(self.db2, "orders"), [
#             (2, "13.37", 2),
#             (5, "51.10", 3),
#             (6, "1.18", 7),
#         ])
#
#     def tearDown(self):
#         os.remove(self.SQLITE_FILE)
#         os.remove(self.SQLITE_FILE2)
#
#     @staticmethod
#     def get_mocked_db_data():
#         class Base:
#             class Metadata:
#                 def __init__(self, sorted_tables):
#                     self.tables = {t.name: t for t in sorted_tables}
#                     self.sorted_tables = sorted_tables
#
#             def __init__(self, sorted_tables):
#                 self.metadata = self.Metadata(sorted_tables)
#
#         class Table:
#             def __init__(self, name):
#                 self.name = name
#
#         class Dict():
#             def __init__(self, dictionary):
#                 for key, val in dictionary.items():
#                     if isinstance(val, dict):
#                         val = Dict(val)
#                     setattr(self, key, val)
#
#             # def __getattr__(self, name):
#             #     return self[name]
#
#         sorted_tables = [
#             Table("users"),
#             Table("orders"),
#         ]
#         base = Base(sorted_tables)
#         return DbData(base, None, None)
#
#     def test_adjust_relationships(self):
#         db = self.get_mocked_db_data()
#         merged_tables = [
#             RowsDict("users", SourceMergeStrategy()),
#             RowsDict("orders", SourceMergeStrategy()),
#         ]
#
#         user_rows_2d = [
#             # source
#             [
#                 (1, "a"),
#                 (2, "b"),
#                 (3, "c"),
#                 (4, "d"),
#             ],
#             # target
#             [
#                 (1, "a"),
#                 (3, "d"),
#                 (5, "b"),
#             ],
#         ]
#         order_rows_2d = [
#             # source
#             [
#                 (1, "o1", 1),
#                 (2, "o2", 3),
#                 # => b
#                 (5, "o3", 2),
#             ],
#             # target
#             [
#                 # => b
#                 (2, "o4", 5),
#             ],
#         ]
#         for user_rows in user_rows_2d:
#             for row in user_rows:
#                 merged_tables[0].put(
#                     hash(row[1:]),
#                     row,
#                     "source"
#                 )
#         for order_rows in order_rows_2d:
#             for row in order_rows:
#                 merged_tables[1].put(
#                     hash(row[1:]),
#                     row,
#                     "source"
#                 )
#
#
#         def strip_ids(rows):
#             return [row[1:] for row in rows]
#
#         adjusted = adjust_relationships(db, merged_tables)
#         self.assertEqual(
#             set(strip_ids(adjusted["users"])),
#             {"a", "b", "c", "d"}
#         )
