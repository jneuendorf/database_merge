from sqlalchemy import create_engine, MetaData, Column, Table
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_utils import database_exists, create_database


# @returns [tuple] The reflected base and the session
# (everything necessary for querying the database).
def get_reflected_db(database_url, prepare_base=True):
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
                "The problem is most likely an inconsistency in the database itself, "
                "e.g. a foreign key constraint that points to a non-existing table."
            ) from e
    return (Base, session, engine)


def merge(input_data):
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
def merge_into_target_db(target_db, reflected_dbs):
    prepare_target_db(target_db)
    for db in reflected_dbs:
        merge_dbs(db, target_db)
    return target_db


# Make sure the database exists. If it already does empty it.
def prepare_target_db(target_db):
    base, session, engine = target_db
    if not database_exists(engine.url):
        print("creating target db")
        create_database(engine.url)
        base.prepare(engine, reflect=True)
    else:
        base.prepare(engine, reflect=True)
        print("clearing target db")
        # base.metadata.drop_all(bind=engine)
        # import pudb; pudb.set_trace()
        print(base.metadata.tables.values())
        for table in reversed(base.metadata.sorted_tables):
            # session.execute(table.delete())
            # session.commit()
            print(table)
        # import sys; sys.exit(1)



# Merges the 2nd database into the 1st.
def merge_dbs(source, target):
    target_base, _, _ = target
    source_base, _, _ = source

    common_tables = (
        set(table_name for table_name in target_base.metadata.tables.keys())
        &
        set(table_name for table_name in source_base.metadata.tables.keys())
    )
    print("common_tables:", common_tables)
    source_tables = source_base.metadata.tables
    for table_name in source_tables:
        source_table = source_tables[table_name]
        if table_name in common_tables:
            print("merging table", table_name)
            merge_tables(
                (source, get_table(target, table_name)),
                (target, source_table),
                table_name
            )
        else:
            print("copying table", table_name)
            copy_table(source, target, source_table)


def get_table(db, table_name):
    base, _, _ = db
    return base.metadata.tables[table_name]


def copy_table(source, target, source_table):
    _, source_session, _ = source
    _, _, target_engine = target
    # This is the query we want to persist in a new table:
    query = source_session.query(source_table)

    # Build the schema for the new table
    # based on the columns that will be returned
    # by the query:
    metadata = MetaData(bind=target_engine)
    columns = [Column(desc['name'], desc['type']) for desc in query.column_descriptions]
    # column_names = [desc['name'] for desc in query.column_descriptions]
    table = Table(source_table.name, metadata, *columns)

    # Create the new table in the destination database
    table.create(target_engine)

    # Finally execute the query
    target_session = Session(target_engine)
    for row in query:
        target_session.execute(table.insert(row))
    target_session.commit()
    # bulk insert:
    # http://docs.sqlalchemy.org/en/latest/orm/session_api.html#sqlalchemy.orm.session.Session.bulk_save_objects
    # http://docs.sqlalchemy.org/en/latest/orm/persistence_techniques.html#bulk-operations
    # s = Session()
    # objects = [
    #     User(name="u1"),
    #     User(name="u2"),
    #     User(name="u3")
    # ]
    # s.bulk_save_objects(objects)
    # s.commit()


# Merges 2 tables with the same structure.
# Algorithm:
# 1. Create a dictionary mapping row hashes to rows.
#    A hashed row is basically the hash of all fields except primary and foreign keys.
#    Primary/foreign keys are later filled with the according values.
def merge_tables(source, target, table_name):
    (source_base, source_session, _), source_table = source
    (target_base, target_session, _), target_table = target
    print(dir(source_table.columns))
    # print(type(source_table.columns), source_table.columns)
    # print(source_table.columns.keys())
    # print(source_table.columns.values())

    if column_types_match(source_table, target_table):
        for column in source_table.columns:
            # column = source_table.columns[column_name]
            # print(type(column), dir(column))
            import pudb; pudb.set_trace()
            print(type(column), column.type, dir(column.type))
        # for row in source_session.query(source_table).all():
        #     print(row)
        #     print(dir(row))
        #     break
    else:
        print(
            f"WARNING: not merging tables with name '{table_name}' "
            "because their column types don't match."
        )


# TODO: order should be irrelevant
def column_types_match(source_table, target_table):
    for source_col in source_table.columns:
        found_match = False
        for target_col in target_table.columns:
            if source_col.type == target_col.type and source_col.name == target_col.name:
                found_match = True
                break
        if not found_match:
            return False
    # TODO: shouldn't be necessary, right?
    # for target_col in target_table.columns:
    #     found_match = False
    #     for source_col in source_table.columns:
    #         if target_col.type == source_col.type and target_col.name == source_col.name:
    #             found_match = True
    #             break
    #     if not found_match:
    #         return False
    return True


def hash_row(row):
    return hash(row[1:])

# # Compare all non-primary-key fields (by value and type).
# def rows_equal(row1, row2):
#     pass
