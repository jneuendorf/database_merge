from strategies import MergeStrategy

class Input():

    def __init__(self, db_urls: list, target_db_url: str, strategy: MergeStrategy) -> None:
        self.db_urls = db_urls
        self.target_db_url = target_db_url
        self.strategy = strategy
