import re
from typing import List

from sqlalchemy import create_engine, inspect, MetaData, Column, Table
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database

from strategies import RowsDict, SourceMergeStrategy
from DbData import DbData
from Input import Input


# @returns [tuple] The reflected base and the session
# (everything necessary for querying the database).
def get_reflected_db(database_url: str, prepare_base=True):
    Base = automap_base()
    try:
        engine = create_engine(database_url)
    except ArgumentError as e:
        raise ValueError(f"Invalid database url '{database_url}'") from e
    session = Session(engine)
    if prepare_base is True:
        try:
            Base.prepare(engine, reflect=True)
        except Exception as e:
            raise RuntimeError(
                f"Error while trying to reflect the database '{database_url}'. "
                "The problem could be that the database server is not running or "
                "there is an inconsistency in the database itself, "
                "e.g. a foreign key constraint that points to a non-existing table."
            ) from e
    return DbData(Base, session, engine)


def merge(input_data: Input):
    merge_into_target_db(
        get_reflected_db(input_data.get_target_db_url(), False),
        [
            get_reflected_db(database_url)
            for database_url in input_data.get_db_urls()
        ]
    )


# Merge N databases into the target database.
# merged_db = merge(merge(merge(db1, db2), db3), db4).
# @param reflected_dbs [list] At least 2 databases.
def merge_into_target_db(target_db: DbData, reflected_dbs: list):
    prepare_target_db(target_db)
    for db in reflected_dbs:
        merge_dbs(db, target_db)
    return target_db


# Make sure the database exists. If it already does empty it.
def prepare_target_db(target_db: DbData):
    base, engine = target_db.base, target_db.engine
    if not database_exists(engine.url):
        print("creating target db")
        create_database(engine.url)
        base.prepare(engine, reflect=True)
    else:
        base.prepare(engine, reflect=True)
        print("clearing target db")
        print("tables before clearing", inspect(engine).get_table_names())
        base.metadata.drop_all(bind=engine)
        # For some reason there are still tables in the metadata
        # even though there are none in the database.
        # for table in reversed(base.metadata.sorted_tables):
        #     session.execute(table.delete())
        #     session.commit()
        #     print(table)
        print("tables after clearing", inspect(engine).get_table_names())


# Merges the 2nd database into the 1st.
def merge_dbs(source: DbData, target: DbData):
    common_tables = (
        set(table_name for table_name in target.inspector.get_table_names())
        &
        set(table_name for table_name in source.inspector.get_table_names())
    )
    print(
        "common_tables of", source.engine.url, "and", target.engine.url, ":",
        sorted(common_tables)
    )
    # TODO: analyse foreign keys:
    # Each database is expected to have valid foreign keys.
    # That means all FKs of a database reference the databas'es tables only.
    # Thus there are 2 dependency graphs (1 for each database).
    # They most likely have tables (in the dep. graph) in common.
    # Since we assume the database's tables to have the same structure we know
    # that tables with the same name also have the same dependencies.
    # EXAMPLE:
    # Graph 1:
    # A -> B -> B ...
    # In the case of cycles (B, e.g. tree structures) assigning primary and
    # foreign keys MUST BE 2 STEPS.
    source_tables = source.base.metadata.tables
    for table_name in source_tables:
        source_table = source_tables[table_name]
        # import pudb; pudb.set_trace()
        if table_name in common_tables:
            print("merging table", table_name)
            merge_tables(source, target, table_name)
        else:
            print("copying table", table_name)
            copy_table(source, target, source_table)


def get_table(db: DbData, table_name: str):
    return db.base.metadata.tables[table_name]


# (Base, session, engine)
def copy_table(source: DbData, target: DbData, source_table: Table):
    # from http://www.tylerlesmann.com/2009/apr/27/copying-databases-across-platforms-sqlalchemy/
    source_meta = MetaData(bind=source.engine)
    table = Table(source_table.name, source_meta, autoload=True)
    table.metadata.create_all(target.engine)
    # NewRecord = quick_mapper(table)
    # columns = table.columns.keys()
    # data = {}
    # import pudb; pudb.set_trace()
    # for record in source.session.query(table).all():
    #     data = dict(
    #         [(str(column), getattr(record, column)) for column in columns]
    #     )
    # print(data)
    # target.session.merge(NewRecord(**data))
    # target.session.commit()

    query = source.session.query(source_table)
    # import pudb; pudb.set_trace()
    # target.session.bulk_save_objects(query.all())
    for row in query:
        # table.insert(row)
        target.session.execute(table.insert(row))
    target.session.commit()
    # # This is the query we want to persist in a new table:
    # query = source.session.query(source_table)
    #
    # # Build the schema for the new table
    # # based on the columns that will be returned
    # # by the query:
    # metadata = MetaData(bind=target.engine)
    # TODO: copy more stuff like primary and foreign keys and more attributes like autoincrement etc
    # # Column('id', ForeignKey('other.id'), primary_key=True, autoincrement='ignore_fk')
    # # columns = [Column(desc['name'], desc['type']) for desc in query.column_descriptions]
    # columns = []
    # for desc in query.column_descriptions:
    #     col = desc['expr']
    #     col_copy = col.copy()
    #     col_copy.foreign_keys = col.foreign_keys
    #     assert(col_copy.foreign_keys is col.foreign_keys)
    #     import pudb; pudb.set_trace()
    #     columns.append(col_copy)
    # # column_names = [desc['name'] for desc in query.column_descriptions]
    # table = Table(source_table.name, metadata, *columns)
    #
    # # Create the new table in the destination database
    # table.create(target.engine)
    #
    # # Finally execute the query
    # target.session = Session(target.engine)
    # # Bulk add isn't that complicated: destSession.add_all(list_of_rows)
    # for row in query:
    #     target.session.execute(table.insert(row))
    # target.session.commit()
    # # bulk insert:
    # http://docs.sqlalchemy.org/en/latest/orm/session_api.html#sqlalchemy.orm.session.Session.bulk_save_objects
    # http://docs.sqlalchemy.org/en/latest/orm/persistence_techniques.html#bulk-operations
    # # s = Session()
    # # objects = [
    # #     User(name="u1"),
    # #     User(name="u2"),
    # #     User(name="u3")
    # # ]
    # # s.bulk_save_objects(objects)
    # # s.commit()

# def quick_mapper(table):
#     Base = declarative_base()
#     class GenericMapper(Base):
#         __table__ = table
#     return GenericMapper


# Merges 2 tables with the same structure (-> columns).
# Algorithm:
# - Make sure the columns are the same (type and name, the order does not matter).
# - Create a dictionary mapping row hashes to rows.
#   A hashed row is basically the hash of all fields except primary and foreign keys.
#   Primary/foreign keys are later filled with the according values.
#   This results in eliminating duplicate rows (without their keys).
# - Fill the dictionary with all rows of both tables.
#   This removes all duplicates.
# - Renew the primary key (assumed to be an integer id!)
#   This is later used for the foreign keys.
def merge_tables(source: DbData, target: DbData, table_name: str):
    source_table = get_table(source, table_name)
    target_table = get_table(target, table_name)

    # source_table.columns.user_id.foreign_keys
    # TODO: pass strategy from user input
    merged_rows = RowsDict(SourceMergeStrategy())

    if table_structures_equal(source_table, target_table):
        for row in source.session.query(source_table).all():
            # print(row)
            # print(dir(row))
            merged_rows.put(
                hash_row(source_table, row),
                row,
                "source"
            )
        print("merged:", merged_rows)
    else:
        print(
            f"WARNING: not merging tables with name '{table_name}' "
            "because their column types don't match."
        )


def table_structures_equal(source_table: Table, target_table: Table):
    # print("table_structures_equal??")
    # print(source_table, target_table)
    num_matching_cols = 0
    for source_col in source_table.columns:
        for target_col in target_table.columns:
            if columns_equal(source_col, target_col):
                # import pudb; pudb.set_trace()
                # print("found a match!", source_col.name)
                num_matching_cols += 1
                break
    # TODO: shouldn't be necessary, right?
    # for target_col in target_table.columns:
    #     found_match = False
    #     for source_col in source_table.columns:
    #         if target_col.type == source_col.type and target_col.name == source_col.name:
    #             found_match = True
    #             break
    #     if not found_match:
    #         return False
    # return True
    print(
        "matching cols for", source_table.name, ":",
        num_matching_cols, "of", len(source_table.columns)
    )
    return num_matching_cols == len(source_table.columns)


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
    regex = re.compile(r"CHARACTER SET .*? COLLATE", re.IGNORECASE)
    str_type1 = regex.sub("COLLATE", str_type1)
    str_type2 = regex.sub("COLLATE", str_type2)
    return str_type1 == str_type2


def hash_row(table: Table, row: tuple) -> int:
    regarded_indices = get_indices_for_hashing(table)
    import pudb; pudb.set_trace()
    # TODO: exclude primary key and/or foreign key (depending on user's input)
    return hash(tuple(
        value for i, value in enumerate(row)
        if i in regarded_indices
    ))


def get_indices_for_hashing(table: Table) -> List[int]:
    return [
        i for i, column in enumerate(table.columns)
        if column.primary_key is False and len(column.foreign_keys) == 0
    ]

# # Compare all non-primary-key fields (by value and type).
# def rows_equal(row1, row2):
#     pass
