from . import MergeStrategy


class SourceMergeStrategy(MergeStrategy):
    """Resolve merge conflicts by taking the values from the source table
    (rather than from the target table).
    Example:

    Schemata:
    User(id, name, contact_person_id)
    ContactPerson(id, name)

    -> Source:
    User:           ContactPerson:
    1 | You | 1     1 | CP1

    -> Target:
    User:           ContactPerson:
    1 | You | 3     3 | CP2

    It is clear what contact person belongs to the user
    but the contact person's name creates a conflict.
    The resulting database should be equal to the source database in this case.

    In this case `merge_tables` must decide to use '1' as `contact_person_id`.
    To do so the relations must be known:
    - 1 user has 1 contact person
    - 1 contact person belongs to N users
    """

    def merge_tables(self, source, target):
        raise NotImplementedError("Must implement 'merge_tables'")
