import logging
from typing import Iterable, List, Tuple

from sqlalchemy import MetaData, Table, Column
from sqlalchemy import create_engine
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

from DbData import DbData
from .private_helpers import columns_equal


# @returns [tuple] The reflected base and the session
# (everything necessary for querying the database).
def get_reflected_db(database_url: str, prepare_base=True) -> DbData:
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


def rereflect(db: DbData) -> None:
    db.base.prepare(db.engine, reflect=True)


def get_table(db: DbData, table_name: str) -> Table:
    return db.base.metadata.tables[table_name]


def truncate_table(db: DbData, table_name: str) -> None:
    # DROP FROM ...
    db.session.execute(get_table(db, table_name).delete())
    db.session.commit()


def insert_rows(target: DbData, table: Table, rows: Iterable[tuple]) -> None:
    """'table' argument cannot be the table name as in the other functions
    because there might be no table object with the wanted name in 'target' yet."""
    for row in rows:
        target.session.execute(table.insert(row))
    target.session.commit()


def get_rows(db: DbData, table_name: str) -> Iterable[tuple]:
    return db.session.query(db.base.metadata.tables[table_name]).all()


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
    logging.debug(
        "matching cols for"
        + source_table.name
        + ":"
        + str(num_matching_cols)
        + "of"
        + str(len(source_table.columns))
    )
    return num_matching_cols == len(source_table.columns)


def copy_table(source: DbData, target: DbData, table_name: str) -> None:
    # from http://www.tylerlesmann.com/2009/apr/27/copying-databases-across-platforms-sqlalchemy/
    source_meta = MetaData(bind=source.engine)
    table = Table(table_name, source_meta, autoload=True)
    table.metadata.create_all(bind=target.engine)

    query = source.session.query(get_table(source, table_name))
    insert_rows(target, table, query.all())


def find_referencing_tables_and_columns(db: DbData, table_name: str) -> List[Tuple[Table, List[Column]]]:
    table = get_table(db, table_name)
    primary_keys = list(table.primary_key.columns)
    sorted_tables = db.base.metadata.sorted_tables
    start_index = sorted_tables.index(table) + 1
    # list of tables that might have a reference to 'table'
    candidates = sorted_tables[start_index:]
    referencing_tables_and_columns = []
    for candidate in candidates:
        referencing_columns = [
            tuple(fk.constraint.columns)[0]
            for i, fk in enumerate(candidate.foreign_keys)
            if fk.column in primary_keys
        ]
        if len(referencing_columns) > 0:
            referencing_tables_and_columns.append(
                (candidate, referencing_columns)
            )

    print("referencing_tables_and_columns for ", table_name, ":", referencing_tables_and_columns)
    # fk_set = table.columns[some].foreign_keys
    # fk.column == referenced column instance (of referenced table)
    return referencing_tables_and_columns
