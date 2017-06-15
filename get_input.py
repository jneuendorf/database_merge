import math
import yaml

from Input import Input

# https://stackoverflow.com/a/20007730/6928824
def ordinal(num):
    start = (math.floor(num/10) % 10 != 1) * (num % 10 < 4) * num % 10
    return "%d%s" % (num, "tsnrhtdd"[
        start::4
    ])


def get_input(cli_args):
    if len(cli_args) > 0 and cli_args[0].startswith("--settings-file="):
        file = open(cli_args[0][16:])
        settings = yaml.safe_load(file)
        file.close()
        if len(settings["db_urls"]) < 2:
            raise ValueError("Hey, merging less than 2 databases is too easy. Bye!")
    else:
        try:
            num_dbs = int(input("Enter number of databases to merge: "))
        except ValueError:
            raise ValueError("Entered invalid number of databases")
        if num_dbs < 2:
            raise ValueError("Hey, merging less than 2 databases is too easy. Bye!")
        print(
            "(database url = "
            "dialect+driver://username:password@host:port/database)"
        )
        i = 1
        db_urls = []
        while num_dbs > 0:
            db_urls.append(
                input(
                    "Enter the database url of "
                    f"the {ordinal(i)} database to be merged: "
                )
            )
            num_dbs -= 1
            i += 1
        settings = {
            "db_urls": db_urls
        }
        settings["target_db_url"] = input(
            "Enter the database url of the target database "
            "that will contain the merged databases: "
        )
    return Input(**settings)
