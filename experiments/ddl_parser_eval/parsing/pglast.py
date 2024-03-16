import pglast
from pglast import prettify
import sqlparse as sqlparser
from .file_handler import FileHandler
import re
from difflib import get_close_matches
from collections import Counter
from pglast.visitors import Visitor

class Stats(Visitor):
    """
    used to count occurences of each node class
    ie dropStmt,createStmt etc
    """
    def __call__(self, node):
        self.counters = Counter()
        super().__call__(node)
        return self.counters

    def visit(self, ancestors, node):
        self.counters.update((node.__class__.__name__,))

class TableNames(Visitor):
    """
    table names used in creates
    returns list of upper-case names
    """
    def __call__(self, node):
        self._table_names = []
        self._schemas = []
        self._dbs = []
        self._views = []
        super().__call__(node)
        return self._table_names,self._schemas,self._dbs,self._views
    
    def visit(self, ancestors, node):
        if isinstance(node,pglast.ast.RangeVar):
            if node.relname is not None:
                self._table_names.append(node.relname.upper())
            if node.schemaname is not None:
                self._schemas.append(node.schemaname.upper())
            if node.catalogname is not None:
                self._dbs.append(node.catalogname.upper())

            if isinstance(abs(ancestors).node,pglast.ast.ViewStmt):
                if node.relname is not None:
                    self._views.append(node.relname.upper())

class SchemaNames(Visitor):
    """
    create schema stmts
    returns list of upper-case names
    """
    def __call__(self, node):
        self._schemas = []
        super().__call__(node)
        return self._schemas
    
    def visit(self, ancestors, node):
        if isinstance(node,pglast.ast.CreateSchemaStmt):
            if node.schemaname is not None:
                self._schemas.append(node.schemaname.upper())

class DBNames(Visitor):
    """
    create db stmts
    returns list of upper-case names
    """
    def __call__(self, node):
        self._dbs = []
        super().__call__(node)
        return self._dbs
    
    def visit(self, ancestors, node):
        if isinstance(node,pglast.ast.CreatedbStmt):
            if node.dbname is not None:
                self._dbs.append(node.dbname.upper())

class ColumnNames(Visitor):
    """
    columns used in creates
    returns list of upper-case column names
    """
    def __call__(self, node):
        self._column_names = []
        super().__call__(node)
        return self._column_names
    
    def visit(self, ancestors, node):
        if isinstance(node,pglast.ast.ColumnDef):
            if node.colname is not None:
                self._column_names.append(node.colname.upper())

class Constraints(Visitor):
    """
    constraints (UNIQUE, FOREIGN, PRIMARY, NOT NULL only)
    returns dict of ints (counts)
    https://pglast.readthedocs.io/en/v4/parsenodes.html#pglast.enums.parsenodes.pglast.enums.parsenodes.ConstrType
    """
    def __call__(self, node):
        self._constraints = {
            'NOTNULL': 0,
            'UNIQUE': 0,
            'PRIMARY': 0,
            'FOREIGN': 0
        }
        super().__call__(node)
        return self._constraints
    
    def visit(self, ancestors, node):
        if isinstance(node,pglast.ast.Constraint):
            if node.contype == pglast.enums.parsenodes.ConstrType.CONSTR_NOTNULL:
                self._constraints['NOTNULL'] += 1
            elif node.contype == pglast.enums.parsenodes.ConstrType.CONSTR_UNIQUE:
                self._constraints['UNIQUE'] += 1
            elif node.contype == pglast.enums.parsenodes.ConstrType.CONSTR_FOREIGN:
                self._constraints['FOREIGN'] += 1
            elif node.contype == pglast.enums.parsenodes.ConstrType.CONSTR_PRIMARY:
                self._constraints['PRIMARY'] += 1   

class PGLast(FileHandler):
    def __init__(self, file_path, encoding):
        """
        reads file contents and closes it
        """

        # inherit file_path, file_id
        super().__init__(file_path)

        
        self.OUTPATH = './out_new/pglast/'
        self.regex = re.compile('[^a-zA-Z]')

        # output cols
        self.num_statements = None
        self.parsed_file = None
        self.file_parse_error = None
        self.original = None
        self.original_highlight = None

        self.file_comment_count = None

        self.num_distinct_tables = None
        self.num_distinct_columns = None
        self.num_distinct_schemas = None
        self.num_distinct_dbs = None
        self.table_list = []
        self.column_list = []
        self.db_list = []
        self.schema_list = []
        self.view_list = []
        self.num_constraints = None
        self.num_ctr_notnull = None
        self.num_ctr_unique = None
        self.num_ctr_primary = None
        self.num_ctr_foreign = None
        self.num_drop_stmt = None
        self.num_drop_like_stmt = None
        self.num_create_stmt = None
        self.num_create_like_stmt = None
        self.num_insert_stmt = None
        self.num_insert_like_stmt = None
        self.num_alter_stmt = None
        self.num_alter_like_stmt = None
        self.num_select_stmt = None
        self.num_view_stmt = None
        self.num_truncate_stmt = None
        self.num_update_stmt = None
        self.num_comment_stmt = None
        self.num_delete_stmt = None

        self.num_set_like_stmt = None
        self.num_execute_like_stmt = None
        self.num_index_like_stmt = None
        self.num_transaction_like_stmt = None

        self.counter_str = None

        self.results = []

        super().file_open(file_encoding=encoding)
        super().file_read()
        super().file_close()
    
    def get_like(self,_like, counter):
        result = 0
        for key in counter:
            if _like.upper() in key.upper() and "stmt".upper() in key.upper():
                result += counter[key]
        return result
    
    def get_comment_count(self, input):
        """
        based on scan: Token(start=0, end=84, name='SQL_COMMENT', kind='NO_KEYWORD')
        get count of comments in an input (file/stmt) 
        """
        comment_count = 0
        for tk in pglast.scan(input):
            if 'COMMENT' in tk.name.upper():
                comment_count += 1
        return comment_count
    
    def get_ctr_str(self,counter):
        _results = []
        for key in counter:
            _results.append({key: counter[key]})
        return str(_results)

    def parse_one(self, stmt, id):
        """
        stmt is string
        id is idx
        """
        _result = {
            "statement_nr": id, 
            "parsed": None, 
            "parse_error": None, 
            "original": None, 
            "original_highlight": None,
            "num_statements": None, 
            "comment_count": None,
            "counter_str": None, 
            "num_distinct_tables": None, 
            "num_distinct_columns": None, 
            "num_distinct_schemas": None, 
            "num_distinct_dbs": None, 
            "table_list": [], 
            "column_list": [], 
            "db_list": [], 
            "schema_list": [],
            "view_list": [],
            "num_constraints": None, 
            "num_ctr_notnull": None, 
            "num_ctr_unique": None, 
            "num_ctr_primary": None, 
            "num_ctr_foreign": None, 
            "num_drop_stmt": None, 
            "num_drop_like_stmt": None, 
            "num_create_stmt": None, 
            "num_create_like_stmt": None, 
            "num_insert_stmt": None, 
            "num_insert_like_stmt": None, 
            "num_alter_stmt": None, 
            "num_alter_like_stmt": None, 
            "num_select_stmt": None, 
            "num_view_stmt": None, 
            "num_truncate_stmt": None, 
            "num_update_stmt": None, 
            "num_comment_stmt": None, 
            "num_delete_stmt": None,
            "num_set_like_stmt": None,
            "num_execute_like_stmt": None, 
            "num_index_like_stmt": None,
            "num_delete_stmt": None,
            "num_transaction_like_stmt": None
        }

        try:
            root = pglast.parse_sql(stmt)
            _result['parsed'] = 1

            stats = Stats()
            cntr = stats(root)
            _result['counter_str'] = self.get_ctr_str(cntr)

            _result['num_statements'] = cntr['RawStmt']

            _result['comment_count'] = self.get_comment_count(stmt)

            _table_list,_schema_list, _db_list, _view_list = TableNames()(root)
            _result['table_list'] = list(set(_table_list))
            _result['num_distinct_tables'] = len(_result['table_list'])

            _result['view_list'] = list(set(_view_list))

            _schema_list = _schema_list + SchemaNames()(root)
            _result['schema_list'] = list(set(_schema_list))
            _result['num_distinct_schemas'] = len(_result['schema_list'])

            _db_list = _db_list + DBNames()(root)
            _result['db_list'] = list(set(_db_list))
            _result['num_distinct_dbs'] = len(_result['db_list'])

            _cols_list = ColumnNames()(root)
            _result['column_list'] = list(set(_cols_list))
            _result['num_distinct_columns'] = len(_result['column_list'])

            _ctrs = Constraints()(root)
            _result['num_constraints'] = cntr['Constraint']
            _result['num_ctr_notnull'] = _ctrs['NOTNULL']
            _result['num_ctr_unique'] = _ctrs['UNIQUE']
            _result['num_ctr_primary'] = _ctrs['PRIMARY']
            _result['num_ctr_foreign'] = _ctrs['FOREIGN']

            _result['num_drop_stmt'] = cntr['DropStmt']
            _result['num_drop_like_stmt'] = self.get_like('drop',cntr)

            _result['num_create_stmt'] = cntr['CreateStmt']
            _result['num_create_like_stmt'] = self.get_like('create',cntr)

            _result['num_insert_stmt'] = cntr['InsertStmt']
            _result['num_insert_like_stmt'] = self.get_like('insert',cntr)

            _result['num_alter_stmt'] = cntr['AlterTableStmt']
            _result['num_alter_like_stmt'] = self.get_like('alter',cntr)

            _result['num_set_like_stmt'] = self.get_like('set',cntr)
            _result['num_execute_like_stmt'] = self.get_like('execute',cntr)
            _result['num_index_like_stmt'] = self.get_like('index',cntr)
            _result['num_transaction_like_stmt'] = self.get_like('transaction',cntr)

            _result['num_select_stmt'] = cntr['SelectStmt']
            _result['num_view_stmt'] = cntr['ViewStmt']
            _result['num_truncate_stmt'] = cntr['TruncateStmt']
            _result['num_update_stmt'] = cntr['UpdateStmt']
            _result['num_comment_stmt'] = cntr['CommentStmt']
            _result['num_delete_stmt'] = cntr['DeleteStmt']

        except pglast.parser.ParseError as e:
            _result['parsed'] = 0
            _result['parse_error'] = str(e)[0:1000]
            _result['original'] = stmt[0:800]

            try:
                pos = int(str(e).split('index')[-1])
                _result['original_highlight'] = stmt[pos-1:]
                _result['original_highlight'] = _result['original_highlight'][0:500]
            except:
                _result['original_highlight'] = None

        return _result

    def parse_file(self):
        
        # trying to parse the whole file
        try:
            root = pglast.parse_sql(self.file_contents)
            self.parsed_file = 1

            stats = Stats()
            cntr = stats(root)
            self.counter_str = self.get_ctr_str(cntr)

            self.num_statements = cntr['RawStmt']

            self.file_comment_count = self.get_comment_count(self.file_contents)

            _table_list,_schema_list, _db_list, _view_list = TableNames()(root)
            self.table_list = list(set(_table_list))
            self.num_distinct_tables = len(self.table_list)

            self.view_list = list(set(_view_list))

            _schema_list = _schema_list + SchemaNames()(root)
            self.schema_list = list(set(_schema_list))
            self.num_distinct_schemas = len(self.schema_list)

            _db_list = _db_list + DBNames()(root)
            self.db_list = list(set(_db_list))
            self.num_distinct_dbs = len(self.db_list)

            _cols_list = ColumnNames()(root)
            self.column_list = list(set(_cols_list))
            self.num_distinct_columns = len(self.column_list)

            _ctrs = Constraints()(root)
            self.num_constraints = cntr['Constraint']
            self.num_ctr_notnull = _ctrs['NOTNULL']
            self.num_ctr_unique = _ctrs['UNIQUE']
            self.num_ctr_primary = _ctrs['PRIMARY']
            self.num_ctr_foreign = _ctrs['FOREIGN']

            self.num_drop_stmt = cntr['DropStmt']
            self.num_drop_like_stmt = self.get_like('drop',cntr)

            self.num_create_stmt = cntr['CreateStmt']
            self.num_create_like_stmt = self.get_like('create',cntr)

            self.num_insert_stmt = cntr['InsertStmt']
            self.num_insert_like_stmt = self.get_like('insert',cntr)

            self.num_alter_stmt = cntr['AlterTableStmt']
            self.num_alter_like_stmt = self.get_like('alter',cntr)

            self.num_set_like_stmt = self.get_like('set',cntr)
            self.num_execute_like_stmt = self.get_like('execute',cntr)
            self.num_index_like_stmt = self.get_like('index',cntr)
            self.num_transaction_like_stmt = self.get_like('transaction',cntr)

            self.num_select_stmt = cntr['SelectStmt']
            self.num_view_stmt = cntr['ViewStmt']
            self.num_truncate_stmt = cntr['TruncateStmt']
            self.num_update_stmt = cntr['UpdateStmt']
            self.num_comment_stmt = cntr['CommentStmt']
            self.num_delete_stmt = cntr['DeleteStmt']
        except pglast.parser.ParseError as e:
            self.parsed_file = 0
            self.file_parse_error = str(e)[0:1200]

            self.original = self.file_contents[0:1500]

            try:
                pos = int(str(e).split('index')[-1])
                self.original_highlight = self.file_contents[pos-1:]
                self.original_highlight = self.original_highlight[0:1500]
            except:
                self.original_highlight = None

        # statement level now

        _idx = 0

        statements = sqlparser.split(self.file_contents)

        for stmt in statements:
            self.results.append(self.parse_one(stmt,_idx))
            _idx += 1

    def get_flat_data(self):
        data = {
            'file_id': self.file_id, # string
            'file_path': self.file_path, # string
            'errors_at_read': self.err_replace, # int 
            'num_statements': self.num_statements, # int
            'parsed_file': self.parsed_file, # int
            'file_parse_error': self.file_parse_error, # string
            'num_distinct_tables': self.num_distinct_tables, # int
            'table_list': self.table_list, # list
            'num_distinct_columns': self.num_distinct_columns, # int
            'columns_list': self.column_list, # list
            'num_distinct_schemas': self.num_distinct_schemas, # int
            'schema_list': self.schema_list, # list
            'num_distinct_dbs': self.num_distinct_dbs, # int
            'db_list': self.db_list, #list
            'view_list': self.view_list, # list
            'num_constraints': self.num_constraints, # int
            'num_ctr_notnull': self.num_ctr_notnull, # int
            'num_ctr_unique': self.num_ctr_unique, # int
            'num_ctr_primary': self.num_ctr_primary, # int
            'num_ctr_foreign': self.num_ctr_foreign, # int
            'counter_str': self.counter_str, # string
            'original': self.original, # string
            'original_highlight': self.original_highlight, # string
            'file_comment_count': self.file_comment_count # int
        }

        if len(self.table_list) == 0:
            data['table_list'] = None
        if len(self.column_list) == 0:
            data['columns_list'] = None
        if len(self.schema_list) == 0:
            data['schema_list'] = None
        if len(self.db_list) == 0:
            data['db_list'] = None
        if len(self.view_list) == 0:
            data['view_list'] = None

        for k in self.__dict__.keys():
            if k not in data.keys() and "num_" in k and "_stmt" in k:
                data[k] = self.__dict__[k]

        statement_list = []
        for item in self.results:

            linked_data = {
                'file_id': self.file_id, # string
                'statement_nr': item['statement_nr'], # int
                'parsed': item['parsed'], # int
                'parse_error': item['parse_error'], # string
                'original': item['original'], # string
                'original_highlight': item['original_highlight'], # string
                'num_statements': item['num_statements'], # int
                'comment_count': item['comment_count'], # int
                'counter_str': item['counter_str'], # string
                'num_distinct_tables': item['num_distinct_tables'], # int
                'num_distinct_columns': item['num_distinct_columns'], # int
                'num_distinct_schemas': item['num_distinct_schemas'], # int
                'num_distinct_dbs': item['num_distinct_dbs'], # int
                'num_constraints': item['num_constraints'] # int
            }

            if len(item['table_list']) == 0:
                linked_data['table_list'] = None
            else:
                linked_data['table_list'] = item['table_list']

            if len(item['column_list']) == 0:
                linked_data['columns_list'] = None
            else:
                linked_data['columns_list'] = item['column_list']

            if len(item['db_list']) == 0:
                linked_data['db_list'] = None
            else:
                linked_data['db_list'] = item['db_list']

            if len(item['schema_list']) == 0:
                linked_data['schema_list'] = None
            else:
                linked_data['schema_list'] = item['schema_list']

            if len(item['view_list']) == 0:
                linked_data['view_list'] = None
            else:
                linked_data['view_list'] = item['view_list']

            for k in item.keys():
                if k not in linked_data.keys(): 
                    if "num_" in k and "_stmt" in k:
                        linked_data[k] = item[k]
                    elif "num_ctr_" in k:
                        linked_data[k] = item[k]

            statement_list.append(linked_data)

        return self.OUTPATH, data, statement_list