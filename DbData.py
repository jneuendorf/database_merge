from sqlalchemy import inspect


class DbData():
    """Contains all data needed to operate on the database."""

    def __init__(self, base, session, engine):
        self.base = base
        self.session = session
        self.engine = engine

    # Need to always get a new inspector so the latest changes are there.
    @property
    def inspector(self):
        return inspect(self.engine)
