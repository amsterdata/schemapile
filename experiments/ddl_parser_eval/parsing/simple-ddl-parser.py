import sqlparse as sqlparser
from .file_handler import FileHandler
import re
from difflib import get_close_matches
from datetime import datetime
from .timeout import timeout

# sort of hack to import both simple-ddl-parsers
import sys
# add first folder to sys.path and import first module
sys.path.append('./sdp/simple_ddl_parser_own')
import simple_ddl_parser as simple_ddl_parser_own

# clean up sys.path and sys.modules
sys.path.remove('./sdp/simple_ddl_parser_own')

to_del = []
for item in sys.modules:
    if "simple_ddl_parser" in item:
        to_del.append(item)
for item in to_del:
    del sys.modules[item]

#add second folder to sys.path and import second module
sys.path.append('./sdp/simple_ddl_parser')
import simple_ddl_parser as simple_ddl_parser

class SimpleDDLParser(FileHandler):
    def __init__(self, file_path, encoding):
        """
        reads file contents and closes it
        """

        # inherit file_path, file_id
        super().__init__(file_path)

        
        self.OUTPATH = './out_new/simpleddlparser/'
        self.regex = re.compile('[^a-zA-Z]')

        # output cols
        self.num_statements = None
        self.parsed_file = None
        self.file_parse_error = None

        self.value_error_present = None

        #self.original = None
        #self.original_highlight = None

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

        #self.counter_str = None

        self.results = []

        super().file_open(file_encoding=encoding)
        super().file_read()
        super().file_close()
    
    def parse_results(self, parse_dict):
        """
        lists are not de-duplicated
        """
        tables = []
        columns = []
        schemas = []
        dbs = []
        num_ctr_unique = 0
        num_ctr_notnull = 0
        num_ctr_primary = 0
        num_ctr_foreign = 0

        num_alter_stmt = 0

        for key, value in parse_dict.items():
            if key == 'tables':
                for tbl in value:
                    for k,v in tbl.items():
                        if k == 'table_name':
                            tables.append(v.upper())
                        if k == 'primary_key':
                            num_ctr_primary += len(v)
                        if k ==  'schema':
                            schemas.append(v)
                        if k == 'alter':
                            for k_alter,v_alter in v.items():
                                for item in v_alter:
                                    num_alter_stmt += 1
                        if k == 'columns':
                            for col in v:
                                for k_col,v_col in col.items():
                                    if k_col == 'name':
                                        columns.append(v_col.upper())
                                    if k_col == 'unique':
                                        if v_col is True:
                                            num_ctr_unique += 1
                                    if k_col == 'nullable':
                                        if v_col is False:
                                            num_ctr_notnull += 1
                                    if k_col == 'references':
                                        if v_col is not None:
                                            num_ctr_foreign += 1
            if key == 'databases':
                for db in value:
                    try:
                        dbs.append(db['database_name'].upper())
                    except KeyError:
                        pass
            if key == 'schemas':
                for sch in value:
                    try:
                        schemas.append(sch['schema_name'].upper())
                    except KeyError:
                        pass

        #filtering Nones
        tables = list(filter(None, tables))
        columns = list(filter(None, columns))
        schemas = list(filter(None, schemas))
        dbs = list(filter(None, dbs))

        # deduplicating lists
        tables = list(set(tables))
        columns = list(set(columns))
        schemas = list(set(schemas))
        dbs = list(set(dbs))

        num_distinct_tables = len(tables)
        num_distinct_columns = len(columns)
        num_distinct_schemas = len(schemas)
        num_distinct_dbs = len(dbs)


        return tables,num_distinct_tables, columns, num_distinct_columns, schemas,num_distinct_schemas, dbs, num_distinct_dbs, num_ctr_unique,num_ctr_notnull,num_ctr_primary,num_ctr_foreign,num_alter_stmt
    
    def parse_one(self, stmt, id):
        """
        stmt is string
        id is idx
        """
        _result = {
            "statement_nr": id, 
            "parsed": None, 
            "parse_error": None,
            "value_error_present": None, 
            "original": None, 
            "num_statements": None, 
            "comment_count": None,
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

        _result['original'] = stmt[0:800]

        try:
            parse_results = simple_ddl_parser.DDLParser(stmt,normalize_names=True,silent=False).run(group_by_type=True)
            _result['parsed'] = 1
            _result['value_error_present'] = 0

            _result['table_list'],_result['num_distinct_tables'],_result['column_list'],_result['num_distinct_columns'],_result['schema_list'],_result['num_distinct_schemas'],_result['db_list'],_result['num_distinct_dbs'],_result['num_ctr_unique'],_result['num_ctr_notnull'],_result['num_ctr_primary'],_result['num_ctr_foreign'],_result['num_alter_stmt'] = self.parse_results(parse_results)           

        except ValueError as e:
            
            try:
                parse_results = simple_ddl_parser_own.DDLParser(stmt,normalize_names=True,silent=False).run(group_by_type=True)
                _result['parsed'] = 1
                _result['value_error_present'] = 1

                _result['table_list'],_result['num_distinct_tables'],_result['column_list'],_result['num_distinct_columns'],_result['schema_list'],_result['num_distinct_schemas'],_result['db_list'],_result['num_distinct_dbs'],_result['num_ctr_unique'],_result['num_ctr_notnull'],_result['num_ctr_primary'],_result['num_ctr_foreign'],_result['num_alter_stmt'] = self.parse_results(parse_results)   
                
            except ValueError as e:
                _result['parsed'] = 0
                _result['value_error_present'] = 1
                
                _result['parse_error'] = 'VALUE_ERROR|' + str(e)[0:1000] + '|'

            except simple_ddl_parser_own.ddl_parser.DDLParserError as e:
                _result['parsed'] = 0
                _result['value_error_present'] = 0
                _result['parse_error'] = 'PARSER_ERROR|' + str(e)[0:1000] + '|'

            except Exception as e:
                _result['parsed'] = 0
                _result['value_error_present'] = 0
                _result['parse_error'] = 'OTHER_ERROR|' + str(e)[0:1000] + '|'
        
        except simple_ddl_parser.ddl_parser.DDLParserError as e:
            _result['parsed'] = 0
            _result['value_error_present'] = 0
            _result['parse_error'] = 'PARSER_ERROR|' + str(e)[0:1000] + '|'
        except Exception as e:
            _result['parsed'] = 0
            _result['value_error_present'] = 0
            _result['parse_error'] = 'OTHER_ERROR|' + str(e)[0:1000] + '|'

        if _result['parsed'] == 0:
            try:
                parse_results = simple_ddl_parser_own.DDLParser(stmt,normalize_names=True,silent=True).run(group_by_type=True)
                _result['parsed'] = 2

                _result['table_list'],_result['num_distinct_tables'],_result['column_list'],_result['num_distinct_columns'],_result['schema_list'],_result['num_distinct_schemas'],_result['db_list'],_result['num_distinct_dbs'],_result['num_ctr_unique'],_result['num_ctr_notnull'],_result['num_ctr_primary'],_result['num_ctr_foreign'],_result['num_alter_stmt'] = self.parse_results(parse_results)  
            except simple_ddl_parser_own.ddl_parser.DDLParserError as e:
                _result['parse_error'] = 'PARSER_ERROR_SILENT|' + str(e)[0:1000] + '|'
            except Exception as e:
                _result['parse_error'] = 'OTHER_ERROR_SILENT|' + str(e)[0:1000] + '|'

        return _result

    def parse_file(self):
        
        try:
            parse_results = simple_ddl_parser.DDLParser(self.file_contents,normalize_names=True,silent=False).run(group_by_type=True)
            self.parsed_file = 1
            self.value_error_present = 0

            self.table_list,self.num_distinct_tables,self.column_list,self.num_distinct_columns,self.schema_list,self.num_distinct_schemas,self.db_list,self.num_distinct_dbs,self.num_ctr_unique,self.num_ctr_notnull,self.num_ctr_primary,self.num_ctr_foreign,self.num_alter_stmt = self.parse_results(parse_results)           

        except ValueError as e:
            
            try:
                parse_results = simple_ddl_parser_own.DDLParser(self.file_contents,normalize_names=True,silent=False).run(group_by_type=True)
                self.parsed_file = 1
                self.value_error_present = 1

                self.table_list,self.num_distinct_tables,self.column_list,self.num_distinct_columns,self.schema_list,self.num_distinct_schemas,self.db_list,self.num_distinct_dbs,self.num_ctr_unique,self.num_ctr_notnull,self.num_ctr_primary,self.num_ctr_foreign,self.num_alter_stmt = self.parse_results(parse_results)   
                
            except ValueError as e:
                self.parsed_file = 0
                self.value_error_present = 1
                self.file_parse_error = 'VALUE_ERROR|' + str(e)[0:1000] + '|'

            except simple_ddl_parser_own.ddl_parser.DDLParserError as e:
                self.parsed_file = 0
                self.value_error_present = 0
                self.file_parse_error = 'PARSER_ERROR|' + str(e)[0:1000] + '|'

            except Exception as e:
                self.parsed_file = 0
                self.value_error_present = 0
                self.file_parse_error = 'OTHER_ERROR|' + str(e)[0:1000] + '|'
        
        except simple_ddl_parser.ddl_parser.DDLParserError as e:
            self.parsed_file = 0
            self.value_error_present = 0
            self.file_parse_error = 'PARSER_ERROR|' + str(e)[0:1000] + '|'
        except Exception as e:
            self.parsed_file = 0
            self.value_error_present = 0
            self.file_parse_error = 'OTHER_ERROR|' + str(e)[0:1000] + '|'

        if self.parsed_file == 0:
            try:
                parse_results = simple_ddl_parser_own.DDLParser(self.file_contents,normalize_names=True,silent=True).run(group_by_type=True)
                self.parsed_file = 2
                self.table_list,self.num_distinct_tables,self.column_list,self.num_distinct_columns,self.schema_list,self.num_distinct_schemas,self.db_list,self.num_distinct_dbs,self.num_ctr_unique,self.num_ctr_notnull,self.num_ctr_primary,self.num_ctr_foreign,self.num_alter_stmt = self.parse_results(parse_results)
            except simple_ddl_parser_own.ddl_parser.DDLParserError as e:
                self.file_parse_error = 'PARSER_ERROR_SILENT|' + str(e)[0:1000] + '|'   
            except Exception as e:
                self.file_parse_error = 'OTHER_ERROR_SILENT|' + str(e)[0:1000] + '|'

        # stmt level now

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
            'value_error_present': self.value_error_present, # int
            'num_distinct_tables': self.num_distinct_tables, # int
            'table_list': self.table_list, # list
            'num_distinct_columns': self.num_distinct_columns, # int
            'column_list': self.column_list, # list
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
            'file_comment_count': self.file_comment_count # int
        }

        if len(self.table_list) == 0:
            data['table_list'] = None
        if len(self.column_list) == 0:
            data['column_list'] = None
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
                'value_error_present': item['value_error_present'], # int
                'original': item['original'], # string
                'num_statements': item['num_statements'], # int
                'comment_count': item['comment_count'], # int
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
                linked_data['column_list'] = None
            else:
                linked_data['column_list'] = item['column_list']

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