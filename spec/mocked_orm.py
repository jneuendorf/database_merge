class Table():

    def __init__(self, name, columns, rows):
        self.name = name
        self.columns = [Column(**column) for column in columns]
        self.rows = [Row(row) for row in rows]

    def __str__(self):
        return (
            "Table(\n" +
            f"  name={self.name},\n" +
            f"  columns={', '.join(str(column) for column in self.columns)}),\n" +
            f"  rows=[...]"
        )


class Column():

    def __init__(self, name, data_type):
        self.name = name
        self.type = data_type

    def __str__(self):
        return f"Column(name={self.name}, type={self.type.name})"


class Row():

    def __init__(self, fields):
        self.fields = fields


# @param tables_data [list] list of dictionaries representing tables
#   [{"name": "some_table", "columns": [], "rows": []}]
def create_test_data(tables_data):
    return [Table(**table_data) for table_data in tables_data]
