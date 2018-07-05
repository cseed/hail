from hail.ir.base_ir import *
from hail.utils.java import escape_str, escape_id

class MatrixRowsTable(BaseIR):
    def __init__(self, child):
        super().__init__()
        self.child = child

    def __str__(self):
        return '(TableRowsTable {})'.format(self.child)

class TableJoin(BaseIR):
    def __init__(self, left, right, join_type):
        super().__init__()
        self.left = left
        self.right = right
        self.join_type = join_type

    def __str__(self):
        return '(TableJoin {} {} {})'.format(
            escape_id(self.join_type), self.left, self.right)

class TableUnion(BaseIR):
    def __init__(self, children):
        super().__init__()
        self.children = children

    def __str__(self):
        return '(TableUnion {})'.format(' '.join([str(x) for x in self.children]))

class TableRange(BaseIR):
    def __init__(self, n, n_partitions):
        super().__init__()
        self.n = n
        self.n_partitions = n_partitions

    def __str__(self):
        return '(TableRange {} {})'.format(self.n, self.n_partitions)

class TaleMapGlobals(BaseIR):
    def __init__(self, child, new_row, value):
        super().__init__()
        self.child = child
        self.new_row = new_row
        self.value = value

    def __str__(self):
        return '(TableMapGlobals {} {} {})'.format(
            escape_str(json.dumps(self.value)),
            self.child, self.new_row)

class TableExplode(BaseIR):
    def __init__(self, child, field):
        super().__init__()
        self.child = child
        self.field = field

    def __str__(self):
        return '(TableExplode {} {})'.format(escape_id(self.field), self.child)

class TableKeyBy(BaseIR):
    def __init__(self, child, keys, n_partitions, sort):
        super().__init__()
        self.child = child
        self.keys = keys
        self.n_partitions = n_partitions
        self.sort = sort

    def __str__(self):
        return '(TableKeyBy ({}) {} {} {})'.format(
            ' '.join([escape_id(x) for x in self.keys]), self.n_partitions, self.sort,
            self.child)

class TableMapRows(BaseIR):
    def __init__(self, child, new_row, new_key, preserved_key_fields):
        super().__init__()
        self.child = child
        self.new_row = new_row
        self.new_key = new_key
        self.preserved_key_fields = preserved_key_fields

    def __str__(self):
        return '(TableMapRows {} {} {} {})'.format(
            ' '.join([escape_id(x) for x in self.new_keys]), self.preserved_key_fields,
            self.child, self.new_row)

class TableUnkey(BaseIR):
    def __init__(self, child):
        super().__init__()
        self.child = child

    def __str__(self):
        return '(TableUnkey {})'.format(self.child)

class TableRead(BaseIR):
    def __init__(self, path, spec, typ, drop_rows):
        super().__init__()
        self.path = path
        self.spec = spec
        self.typ = typ
        self.drop_rows = drop_rows

    def __str__(self):
        return '(TableRead {} {} {} {})'.format(
            escape_str(self.path),
            escape_str(json.dumps(self.spec)),
            self.typ._jtype.parsableString(),
            self.drop_rows)

class TableImport(BaseIR):
    def __init__(self, paths, typ, reader_options):
        super().__init__()
        self.paths = paths
        self.typ = typ
        self.reader_options = reader_options

    def __str__(self):
        return '(TableImport ({}) {} {})'.format(
            ' '.join([escape_str(path) for path in self.paths]),
            self.typ._jtype.parsableString(),
            escape_str(json.dumps(self.reader_options)))

class MatrixEntriesTable(BaseIR):
    def __init__(self, child):
        super().__init__()
        self.child = child

    def __str__(self):
        return '(MatrixEntriesTable {})'.format(self.child)

class TableFilter(BaseIR):
    def __init__(self, child, pred):
        super().__init__()
        self.child = child
        self.pred = pred

    def __str__(self):
        return '(TableFilter {})'.format(self.child, self.pred)

class TableAggregateByKey(BaseIR):
    def __init__(self, child, expr):
        super().__init__()
        self.child = child
        self.expr = expr

    def __str__(self):
        return '(TableAggregateByKey {} {})'.format(self.child, self.expr)

class TableColsTable(BaseIR):
    def __init__(self, child):
        super().__init__()
        self.child = child

    def __str__(self):
        return '(TableColsTable {})'.format(self.child)

class TableParallelize(BaseIR):
    def __init__(self, typ, rows, n_partitions):
        super().__init__()
        self.typ = typ
        self.rows = rows
        self.n_partitions = n_partitions

    def __str__(self):
        return '(TableParallelize {} {} {})'.format(
            self.typ._jtype.parsableString(),
            escape_str(json.dumps(self.rows)),
            self.n_partitions)
