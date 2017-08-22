import logging
from typing import Any, Dict, Iterable, List

# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect, Table
from sqlalchemy_utils import database_exists, create_database

import db_helpers
from strategies import RowsDict, SourceMergeStrategy
from DbData import DbData
from Input import Input
import value_generators


def merge(input_data: Input) -> DbData:
    merge_into_target_db(
        db_helpers.get_reflected_db(input_data.target_db_url, False),
        [
            db_helpers.get_reflected_db(database_url)
            for database_url in input_data.db_urls
        ]
    )
    return db_helpers.get_reflected_db(input_data.target_db_url, True)


# Merge N databases into the target database.
# merged_db = merge(merge(merge(db1, db2), db3), db4).
# @param reflected_dbs [list] At least 2 databases.
def merge_into_target_db(target_db: DbData, reflected_dbs: Iterable[DbData]):
    prepare_target_db(target_db)
    for db in reflected_dbs:
        merge_dbs(db, target_db)
        db_helpers.rereflect(target_db)
    return target_db


# Make sure the database exists. If it already does empty it.
def prepare_target_db(target_db: DbData):
    base, engine = target_db.base, target_db.engine
    if not database_exists(engine.url):
        print("creating target db")
        logging.debug(f"creating target database {engine.url}")
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


def merge_dbs(source: DbData, target: DbData) -> None:
    common_tables = (
        set(table_name for table_name in source.inspector.get_table_names())
        &
        set(table_name for table_name in target.inspector.get_table_names())
    )
    logging.debug(
        f"common_tables of {source.engine.url} and {target.engine.url}:"
        + str(sorted(common_tables))
    )
    # TODO: analyze foreign keys:
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

    merged_tables = []

    # The sorting will place Table objects that have dependencies first,
    # before the dependencies themselves, representing the order
    # in which they can be created.
    source_tables = reversed(source.base.metadata.sorted_tables)

    for source_table in source_tables:
        table_name = source_table.name
        # source_table = source_tables[table_name]
        if table_name in common_tables:
            print("merging table", table_name)
            merged_tables.append(merge_tables(source, target, table_name))
        else:
            print("copying table", table_name)
            db_helpers.copy_table(source, target, table_name)

    merged_and_adjusted_relations_tables = adjust_relationships(target, merged_tables)
    for table_name, rows_to_insert in merged_and_adjusted_relations_tables.items():
        db_helpers.truncate_table(target, table_name)
        db_helpers.insert_rows(
            target,
            db_helpers.get_table(target, table_name),
            rows_to_insert
        )


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
def merge_tables(source: DbData, target: DbData, table_name: str) -> RowsDict:
    source_table = db_helpers.get_table(source, table_name)
    target_table = db_helpers.get_table(target, table_name)

    # source_table.columns.user_id.foreign_keys
    # TODO: pass strategy from user input
    merged_rows = RowsDict(table_name=table_name, strategy=SourceMergeStrategy())

    if db_helpers.table_structures_equal(source_table, target_table):
        for row in source.session.query(source_table).all():
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
        # rows = adjust_relationships(merged_rows)
        # # truncating does not work...
        # db_helpers.truncate_table(target, table_name)
        # db_helpers.insert_rows(target, target_table, rows)
    else:
        print(
            f"WARNING: not merging tables with name '{table_name}' "
            "because their structures are not equal."
        )
    return merged_rows


def hash_row(table: Table, row: tuple) -> int:
    regarded_indices = get_indices_for_hashing(table)
    hash_value = hash(tuple(
        value for i, value in enumerate(row)
        if i in regarded_indices
    ))
    # print("hash for row", row, "=", hash_value)
    return hash_value


def get_indices_for_hashing(table: Table) -> List[int]:
    return [
        i for i, column in enumerate(table.columns)
        if column.primary_key is False and len(column.foreign_keys) == 0
    ]


def adjust_relationships(db: DbData, merged_tables: List[RowsDict]) -> Dict[str, List[Any]]:
    """
    Reassigns e.g. primary keys so relationships stay intact.
    The keys are adjusted in the merged_rows RowsDict.

    The algorithm works like so:

    TL <- list of all tables (sorted in drop order)
    for each table T in TL do
    begin
        RD <- RowsDict with the same table name as T
        L <- [] (list of rows (and their old primary keys) to insert later)
        for each row R in RD do
        begin
            P <- current primary key
            generate a new primary key P' for R
            update R with P'
            append (R, P) to L
        end
    end

    After this phase all rows have new primary keys and know their old ones.
    We use that information to correct the currently broken foreign keys
    (due to the new primary keys).

    for each table T in reversed(TL) do
    begin
        DL <- list of dependent tables (e.g. for a user table
         find all tables that have a user id)
        for each D in DL do
        begin
            for row R in D's rows do
            begin
                find related row X in T (the one where the referencing column
                 has the old primary key)
                update the relation
            end
        end
    end
    """

    table_rows_by_name = {}
    print("merged_tables:", [str(t) for t in merged_tables])
    merged_tables_by_name = {
        merged_table.table_name: merged_table
        for merged_table in merged_tables
    }
    sorted_tables = [
        table
        for table in db.base.metadata.sorted_tables
        if table.name in merged_tables_by_name
    ]

    # Iterate tables from tables WITH foreign keys to tables WITHOUT foreign keys.
    # -> Order in which the tables could be deleted.
    for table in reversed(sorted_tables):
        table_name = table.name
        merged_table = merged_tables_by_name[table_name]

        # ASSUMPTION: IDs as primary keys in 1st column
        id_generator = value_generators.value_generator_for_type(int)
        rows_by_old_pks = {}
        for row, primary_keys in merged_table.get_rows_with_pks():
            # row = list(row)
            # old_pk = row[0]
            row[0] = next(id_generator)
            if primary_keys not in rows_by_old_pks:
                rows_by_old_pks[primary_keys] = []
            rows_by_old_pks[primary_keys].append(row)
        table_rows_by_name[table_name] = rows_by_old_pks

    for referenced_table in sorted_tables:
        referenced_table_name = referenced_table.name
        referencing_tables = db_helpers.find_referencing_tables_and_columns(db, referenced_table_name)
        for referencing_table, referencing_columns in referencing_tables:
            # old_pks contains the primary keys before the merge
            for _, referencing_rows in table_rows_by_name[referencing_table.name].items():
                for referencing_column in referencing_columns:
                    ref_col_name = referencing_column.name
                    fk_idx = [
                        i
                        for i, col in enumerate(referencing_table.columns)
                        if col.name == ref_col_name
                    ][0]

                    for referencing_row in referencing_rows:
                        # This is the primary key from after the merge
                        # but still pointing to the primary key from before the merge.
                        fk = referencing_row[fk_idx]
                        referenced_row = None
                        for old_pks in table_rows_by_name[referenced_table_name]:
                            # TODO: this is a list of rows instead of a row!
                            # If we have multiple rows that are referenced how do we know which one is the wanted one?
                            potentially_referenced_rows = table_rows_by_name[referenced_table_name][old_pks]
                            if fk in old_pks:
                                referenced_row = potentially_referenced_rows
                                break

                        if referenced_row is None:
                            raise ValueError(
                                f"Could not find a row with primary key {fk} "
                                f"in {referenced_table_name}"
                            )

                        # update row's foreign key value
                        # ASSUMPTION: IDs as primary keys in 1st column
                        referencing_row[fk_idx] = referenced_row[0]

    import pudb; pudb.set_trace()
    # drop meta info about old primary keys
    return {
        table_name: [row for old_pks, row in rows_by_old_pks.items()]
        for table_name, rows_by_old_pks in table_rows_by_name.items()
    }
    # sorted_tables
    # fk_set = table.columns[some].foreign_keys
    # fk.column == referenced column instance (of referenced table)
