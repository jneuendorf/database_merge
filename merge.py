from typing import List, Iterable

from sqlalchemy import create_engine, inspect, Table
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database

import db_helpers
from strategies import RowsDict, SourceMergeStrategy
from DbData import DbData
from Input import Input
import value_generators


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
def merge_dbs(source: DbData, target: DbData) -> None:
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
        # if table_name == "full_applications":
        #     import pudb; pudb.set_trace()
        if table_name in common_tables:
            print("merging table", table_name)
            merge_tables(source, target, table_name)
        else:
            print("copying table", table_name)
            db_helpers.copy_table(source, target, source_table)


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
    source_table = db_helpers.get_table(source, table_name)
    target_table = db_helpers.get_table(target, table_name)

    # source_table.columns.user_id.foreign_keys
    # TODO: pass strategy from user input
    merged_rows = RowsDict(table_name=table_name, strategy=SourceMergeStrategy())

    if db_helpers.table_structures_equal(source_table, target_table):
        # if table_name == "admins":
        #     import pudb; pudb.set_trace()
        for row in source.session.query(source_table).all():
            # print(row)
            # print(dir(row))
            merged_rows.put(
                hash_row(source_table, row),
                row,
                "source"
            )
        for row in target.session.query(target_table).all():
            merged_rows.put(
                hash_row(target_table, row),
                row,
                "target"
            )
        print("merged:", merged_rows)
        rows = adjust_relationships(merged_rows)
        import pudb; pudb.set_trace()
        # truncating does not work...
        db_helpers.truncate_table(target, table_name)
        db_helpers.insert_rows(target, target_table, rows)
    else:
        print(
            f"WARNING: not merging tables with name '{table_name}' "
            "because their structures are not equal."
        )


def hash_row(table: Table, row: tuple) -> int:
    regarded_indices = get_indices_for_hashing(table)
    hash_value = hash(tuple(
        value for i, value in enumerate(row)
        if i in regarded_indices
    ))
    print("hash for row", row, "=", hash_value)
    return hash_value


def get_indices_for_hashing(table: Table) -> List[int]:
    return [
        i for i, column in enumerate(table.columns)
        if column.primary_key is False and len(column.foreign_keys) == 0
    ]


def adjust_relationships(merged_rows: RowsDict) -> Iterable[tuple]:
    """Reassigns e.g. primary keys so relationships stay the same.
    The 'source' and 'target' arguments are needed to determine the current
    relationships.
    The keys are adjusted in the merged_rows RowsDict."""
    id_generator = value_generators.value_generator_for_type(int)
    rows_to_insert = []
    for row in merged_rows.get_rows():
        row = list(row)
        row[0] = next(id_generator)
        rows_to_insert.append(tuple(row))
    return rows_to_insert
    # sorted_tables
    # fk_set = table.columns[some].foreign_keys
    # fk.column == referenced column instance (of referenced table)
