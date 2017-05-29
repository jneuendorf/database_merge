import os
import sys

from get_input import get_input
from merge import get_reflected_dbs, merge_databases


if __name__ == '__main__':
    input_data = get_input(sys.argv[1:], sys.exit)
    reflected_databases = get_reflected_dbs(input_data)
    # print(reflected_databases)
    merge_into_target_db(
        input_data.get_target_database(),
        reflected_databases
    )
