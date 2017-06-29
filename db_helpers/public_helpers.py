from typing import Iterable

from sqlalchemy import MetaData, Table
from sqlalchemy import create_engine
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

from DbData import DbData
from .private_helpers import columns_equal


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


def get_table(db: DbData, table_name: str) -> Table:
    return db.base.metadata.tables[table_name]


def truncate_table(db: DbData, table_name: str) -> None:
    # DROP FROM ...
    db.session.execute(get_table(db, table_name).delete())
    db.session.commit()


def insert_rows(target: DbData, table: Table, rows: Iterable[tuple]) -> None:
    for row in rows:
        target.session.execute(table.insert(row))
    target.session.commit()


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


# (Base, session, engine)
def copy_table(source: DbData, target: DbData, source_table: Table) -> None:
    # from http://www.tylerlesmann.com/2009/apr/27/copying-databases-across-platforms-sqlalchemy/
    source_meta = MetaData(bind=source.engine)
    table = Table(source_table.name, source_meta, autoload=True)
    table.metadata.create_all(bind=target.engine)

    query = source.session.query(source_table)
    # # target.session.bulk_save_objects(query.all())
    # for row in query:
    #     # table.insert(row)
    #     target.session.execute(table.insert(row))
    # target.session.commit()
    insert_rows(target, table, query.all())

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
