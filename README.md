# database-merge

Merge 2 databases with the same structure while keeping foreign keys constraints.
When merging 2 tables  rows are considered equal when all columns except the primary key are equal.
Thus we assume that the primary key is something like `id : int` and has no other purpose than being an identifier
(like having a primary key consisting of first and last name, for example).
