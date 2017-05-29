import math
import yaml


# https://stackoverflow.com/a/20007730/6928824
def ordinal(n):
    start = (math.floor(n/10) % 10 != 1) * (n % 10 < 4) * n % 10
    return "%d%s" % (n, "tsnrhtdd"[
        start::4
    ])


def get_input(cli_args, exit):
    if len(cli_args) > 0 and cli_args[0].startswith("--settings-file="):
        file = open(cli_args[0][16:])
        settings = yaml.safe_load(file)
        file.close()
        if len(settings["db_urls"]) <= 1:
            print("Hey, this is too easy. Bye!")
            exit(1)
    else:
        try:
            n = int(input("Enter number of databases to merge: "))
        except ValueError as e:
            print("Entered invalid number of databases. Exiting...")
            exit(1)
        if n <= 1:
            print("Hey, this is too easy. Bye!")
            exit(1)
        print(
            "(database url = "
            "dialect+driver://username:password@host:port/database)"
        )
        i = 1
        db_urls = []
        while n > 0:
            db_urls.append(
                input(
                    "Enter the database url of "
                    f"the {ordinal(i)} database to be merged: "
                )
            )
            n -= 1
            i += 1
        settings = {
            "db_urls": db_urls
        }

    default_target_db = '_'.join(
        url.split("/")[-1]
        for url in settings['db_urls']
    )
    target_db = input(
        "Enter the name of the target database "
        "that will contain the merged databases "
        f"(default=merged_{default_target_db}): "
    )
    settings["target_db"] = target_db if len(target_db) > 0 else default_target_db
    return Input(**settings)


class Input():

    def __init__(self, db_urls, target_db):
        print("> db_urls", db_urls)
        self.db_urls = db_urls
        self.target_db = target_db

    def get_db_urls(self):
        return self.db_urls
