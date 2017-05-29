from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


# @param input_data [Input]
def get_reflected_dbs(input_data):
    # Base = get_reflected_db(input_data.database_urls[0])
    # for name, table in Base.metadata.tables.items():
    #     print(name)
    #     print(table.primary_key)
    #     # print(table.columns)
    #     print()
    # Admin = Base.classes.admins
    #
    # queryset = session.query(Admin.id, Admin.first_name, Admin.email)
    # for id, first_name, email in queryset:
    #     print(id, first_name, email)
    return [
        get_reflected_db(database_url)
        for database_url in input_data.get_db_urls()
    ]


def get_reflected_db(database_url):
    Base = automap_base()
    engine = create_engine(database_url)
    session = Session(engine)
    Base.prepare(engine, reflect=True)
    return Base


# Merge N databases into the target database.
# merged_db = merge(merge(merge(db1, db2), db3), db4).
# @param reflected_dbs [list] At least 2 databases.
def merge_into_target_db(target_db, reflected_dbs):
    merged_db = merge_dbs(*reflected_dbs[0:2])
    for db in reflected_dbs[2:]:
        merged_db = merge_dbs(merged_db, db)
    return merged_db


# Merges 2 databases.
def merge_dbs(db1, db2):
    return db1
