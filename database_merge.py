import sys

from get_input import get_input
from merge import merge


if __name__ == '__main__':
    try:
        from merge import get_reflected_db
        Base2, session2, _ = get_reflected_db("mysql://root:@localhost:3306/db_merge_test2")
        # sys.exit(0)
        Base1, session1, _ = get_reflected_db("mysql://root:@localhost:3306/db_merge_test")
        common_tables = (
            set(table_name for table_name in Base1.metadata.tables.keys())
            &
            set(table_name for table_name in Base2.metadata.tables.keys())
        )
        print(common_tables)
        # for name, table in Base1.metadata.tables.items():
        #     print(name)
        #     print(table.primary_key)
        #     # print(table.columns)
        #     print()
        # Admin = Base1.classes.admins
        #
        # queryset = session1.query(Admin.id, Admin.first_name, Admin.email)
        # for _id, first_name, email in queryset:
        #     print(_id, first_name, email)
    except Exception as e:
        print(str(e))

    merge(get_input(sys.argv[1:]))
    # try:
    #     merge(get_input(sys.argv[1:]))
    # except Exception as e:
    #     print(str(e))
