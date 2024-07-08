import sqlglot as sqlglot
import sqlparse as sqlparser
from file_handler import FileHandler
import re
from difflib import get_close_matches
from datetime import datetime
from timeout import timeout
from collections import Counter
from sqlglot.dialects import Dialects

class SQLGlot(FileHandler):
    def __init__(self, file_path, encoding):
        """
        reads file contents and closes it
        """

        # inherit file_path, file_id
        super().__init__(file_path)

        
        self.OUTPATH = './out_new/sqlglot/'
        self.regex = re.compile('[^a-zA-Z]')
        self.cntr = Counter()
        self.postgres_cntr = Counter()

        # order matters
        # removing couple of them because it takes too long
        self.mandatory_dialects = ["postgres"]
        self.other_dialects = []
        #self.mandatory_dialects = [None, "postgres","mysql","tsql"]
        #self.other_dialects = [
        #    "oracle", "sqlite",
        #    "bigquery", "duckdb", "hive", "presto", "redshift", "teradata",
        #    "snowflake", "databricks" 
        #]

        self.sqlglot_attributes = [
            'parsed', 'parsed_none', 'parsed_postgres', 'parsed_mysql', 'parsed_tsql', 'dialect', 'parse_error',
            'file_parse_error_start_context', 'file_parse_error_highlight'
            ]
        
        self.output_attributes = [
            'num_statements','num_distinct_tables', 'num_distinct_columns', 'num_distinct_schemas', 'num_distinct_dbs',
            'table_list','column_list', 'db_list', 'schema_list', 'view_list', 'num_constraints', 'num_ctr_notnull',
            'num_ctr_unique', 'num_ctr_primary','num_ctr_foreign', 'comment_count', 'num_drop_stmt', 'num_drop_like_stmt',
            'num_create_stmt', 'num_create_like_stmt','num_insert_stmt', 'num_insert_like_stmt', 'num_alter_stmt', 
            'num_alter_like_stmt', 'num_select_stmt', 'num_view_stmt','num_truncate_stmt', 'num_update_stmt', 
            'num_comment_stmt', 'num_delete_stmt', 'num_set_like_stmt', 'num_execute_like_stmt','num_index_like_stmt', 
            'num_transaction_like_stmt', 'num_command_stmt', 'num_command_alter_stmt'
            ]

        self.file_output_dict = {}

        for k in self.sqlglot_attributes:
            self.file_output_dict[k] = None

        for k in self.output_attributes:
            if '_list' in k:
                self.file_output_dict[k] = []
                self.file_output_dict["postgres_" + k] = []
            else:
                self.file_output_dict[k] = 0
                self.file_output_dict["postgres_" + k] = 0

        self.file_output_dict['counter_str'] = None
        self.file_output_dict['postrgres_counter_str'] = None

        self.results = []

        super().file_open(file_encoding=encoding)
        super().file_read()
        super().file_close()
    
    def get_tables(self, stmt):
        """
        inexplicably columns mentioned in alter tables get returned as tables
        possibly fixed
        """
        _out = []
        for tbl in stmt.find_all(sqlglot.exp.Table):
            try:
                
                possible_column = str(tbl.parent.dump()['args']['kind'])
                if possible_column.upper() != 'COLUMN' and possible_column.upper() != 'DATABASE' and possible_column.upper() != 'SCHEMA' and possible_column.upper() != 'CATALOG' and possible_column.upper() != 'INDEX':
                    _out.append(tbl.name.upper())
            except KeyError:
                _out.append(tbl.name.upper())

        # handling foreign key references    
        for ref in stmt.find_all(sqlglot.exp.Reference):
            _out.append(ref.find(sqlglot.exp.Identifier).name.upper())
        
        # filtering zero length elements (case from generated bla bla in 000835_functions)
        _out = list(filter(None, _out))
        return _out

    def get_columns(self, stmt):
        """
        restricting to ColumnDef because this is what's taken with pglast
        """
        _out = []
        #for col in stmt.find_all(sqlglot.exp.Column):
        #    _out.append(col.name.upper())
        for col in stmt.find_all(sqlglot.exp.ColumnDef):
            _out.append(col.name.upper())
        #for col in stmt.find_all(sqlglot.exp.AlterColumn):
        #    _out.append(col.name.upper())
        return _out
    
    def get_views(self, stmt):
        _out = []
        for tbl in stmt.find_all(sqlglot.exp.Create):
            try:
                if tbl.dump()['args']['kind'].upper() == 'VIEW':
                    _out.append(tbl.find(sqlglot.exp.Table).name.upper())
            except KeyError:
                pass
        return _out
    
    def get_schemas(self, stmt):
        _out = []
        for tbl in stmt.find_all(sqlglot.exp.Create):
            try:
                if tbl.dump()['args']['kind'].upper() == 'SCHEMA':
                    _out.append(tbl.find(sqlglot.exp.Table).name.upper())
            except KeyError:
                pass
        return _out
    
    def get_dbs(self, stmt):
        _out = []
        for tbl in stmt.find_all(sqlglot.exp.Create):
            try:
                if tbl.dump()['args']['kind'].upper() == 'DATABASE':
                    _out.append(tbl.find(sqlglot.exp.Table).name.upper())
            except KeyError:
                pass
        return _out

    def get_constraint_count(self, stmt):
        _out = 0
        for col in stmt.find_all(sqlglot.exp.ColumnConstraint):
            _out += 1
        return _out

    def get_constraints(self, stmt):
        _constraints = {
            'NOTNULL': 0,
            'UNIQUE': 0,
            'PRIMARY': 0,
            'FOREIGN': 0
        }
        for col in stmt.find_all(sqlglot.exp.NotNullColumnConstraint):
            _constraints['NOTNULL'] += 1
        for col in stmt.find_all(sqlglot.exp.PrimaryKey):
            _constraints['PRIMARY'] += 1        
        for col in stmt.find_all(sqlglot.exp.UniqueColumnConstraint):
            _constraints['UNIQUE'] += 1
        for col in stmt.find_all(sqlglot.exp.ForeignKey):
            _constraints['FOREIGN'] += 1
        return _constraints
    
    def count_type(self, stmt, cls):
        """
        create: sqlglot.exp.Create
        insert: sqlglot.exp.Insert
        alter: AlterTable
        alter_like: AlterTable + AlterColumn
        Select
        Update
        Delete
        Comment
        set_like: Set + SetItem + SetProperty
        Index
        transaction
        """
        cnt = 0
        for n in stmt.find_all(cls):
            cnt += 1
        return cnt
    
    def count_type_exclude(self, stmt, cls, kind_to_exclude):
        """
        for drop table call with: (stmt, sqlglot.exp.Drop, "COLUMN")
        """
        cnt = 0
        for n in stmt.find_all(cls):
            try:
                kind = n.dump()['args']['kind']
            except KeyError:
                kind = ''
            if kind.upper() != kind_to_exclude.upper():
                cnt += 1
        return cnt
    
    def get_truncate_cnt(self,stmt):
        cnt = 0
        for n in stmt.find_all(sqlglot.exp.Command):
            try:
                kind = n.dump()['args']['this']
            except KeyError:
                kind = ''
            if kind.upper() == 'TRUNCATE'.upper():
                cnt += 1
        return cnt
    
    def get_command_count(self,stmt):
        cnt = 0
        for n in stmt.find_all(sqlglot.exp.Command):
            try:
                kind = n.dump()['args']['this']
            except KeyError:
                kind = ''
            if kind.upper() != 'TRUNCATE'.upper() and kind.upper != 'ALTER'.upper():
                cnt += 1
        return cnt

    def get_command_alter_count(self,stmt):
        cnt = 0
        for n in stmt.find_all(sqlglot.exp.Command):
            try:
                kind = n.dump()['args']['this']
            except KeyError:
                kind = ''
            if kind.upper() == 'ALTER'.upper():
                cnt += 1
        return cnt

    def get_execute_cnt(self,stmt):
        cnt = 0
        for n in stmt.find_all(sqlglot.exp.Command):
            try:
                kind = n.dump()['args']['this']
            except KeyError:
                kind = ''
            if kind.upper() == 'EXECUTE'.upper():
                cnt += 1
        return cnt
    
    def get_comment_count(self, sql):
        """
        sql is original string
        """
        cnt = 0
        tk = sqlglot.Tokenizer()
        for tkn in tk.tokenize(sql):
            # tkn.comments is a list
            if len(tkn.comments) > 0:
                cnt += 1
        return cnt
    
    def update_counter(self, stmt, cntr):
        for node in stmt.walk():
            cntr.update((self.regex.sub('',str(type(node[0])).split("sqlglot.expressions")[-1]),))

    def get_ctr_str(self,counter):
        _results = []
        for key in counter:
            _results.append({key: counter[key]})
        return str(_results)
    
    def parse_one(self, stmt, id=None):
        """
        stmt is sqlglot ast for 1 single statement
        id is idx, None if file level
        """
        
        _result = {}

        # when doing stmt level
        if id is not None:
            _result["statement_id"] = id

        for k in self.sqlglot_attributes:
            _result[k] = None

        for k in self.output_attributes:
            if '_list' in k:
                _result[k] = []
            else:
                _result[k] = 0

        
        _result['table_list'] = self.get_tables(stmt)
        _result['table_list'] = list(set(_result['table_list']))
        _result['num_distinct_tables'] = len(_result['table_list'])

        _result['column_list'] = self.get_columns(stmt)
        _result['column_list'] = list(set(_result['column_list']))
        _result['num_distinct_columns'] = len(_result['column_list'])

        _result['db_list'] = self.get_dbs(stmt)
        _result['db_list'] = list(set(_result['db_list']))
        _result['num_distinct_dbs'] = len(_result['db_list'])

        _result['schema_list'] = self.get_schemas(stmt)
        _result['schema_list'] = list(set(_result['schema_list']))
        _result['num_distinct_schemas'] = len(_result['schema_list'])

        _result['view_list'] = self.get_views(stmt)
        _result['view_list'] = list(set(_result['view_list']))
        _result['num_view_stmt'] = len(_result['view_list'])

        _result['num_constraints'] = self.get_constraint_count(stmt)
        _constraints = self.get_constraints(stmt)
        _result['num_ctr_notnull'] = _constraints['NOTNULL']
        _result['num_ctr_unique'] = _constraints['UNIQUE']
        _result['num_ctr_primary'] = _constraints['PRIMARY']
        _result['num_ctr_foreign'] = _constraints['FOREIGN']

        _result['num_drop_stmt'] = self.count_type_exclude(stmt,sqlglot.exp.Drop,'COLUMN')
        _result['num_drop_like_stmt'] = _result['num_drop_stmt']
        
        _result['num_truncate_stmt'] = self.get_truncate_cnt(stmt)
        _result['num_execute_like_stmt'] = self.get_execute_cnt(stmt)

        
        _result['num_create_stmt'] = self.count_type(stmt,sqlglot.exp.Create)
        _result['num_create_like_stmt'] = _result['num_create_stmt']
        
        _result['num_insert_stmt'] = self.count_type(stmt,sqlglot.exp.Insert)
        _result['num_insert_like_stmt'] = _result['num_insert_stmt']

        _result['num_alter_stmt'] = self.count_type(stmt,sqlglot.exp.AlterTable)
        _result['num_alter_like_stmt'] = _result['num_alter_stmt'] + self.count_type(stmt,sqlglot.exp.AlterColumn)

        _result['num_select_stmt'] = self.count_type(stmt,sqlglot.exp.Select)
        
        _result['num_update_stmt'] = self.count_type(stmt,sqlglot.exp.Update)
        _result['num_delete_stmt'] = self.count_type(stmt,sqlglot.exp.Delete)

        _result['num_comment_stmt'] = self.count_type(stmt,sqlglot.exp.Comment)
        _result['num_transaction_like_stmt'] = self.count_type(stmt,sqlglot.exp.Transaction)
        _result['num_index_like_stmt'] = self.count_type(stmt,sqlglot.exp.Index)
        
        
        _result['num_set_like_stmt'] = self.count_type(stmt,sqlglot.exp.Set) + self.count_type(stmt,sqlglot.exp.SetItem) + self.count_type(stmt,sqlglot.exp.SetProperty)

        _result['num_command_stmt'] = self.get_command_count(stmt)
        _result['num_command_alter_stmt'] = self.get_command_alter_count(stmt)
        
        return _result

    def update_output_dict(self, dialect, parse_result):
        if dialect in self.mandatory_dialects:
            if dialect is None:
                self.file_output_dict["parsed_none"] = parse_result
            else:
                self.file_output_dict["parsed_" + dialect] = parse_result
        else:
            if parse_result == 1:
                self.file_output_dict["dialect"] = dialect

    def update_dict_parse_error(self, parse_err, parse_err_start, parse_err_highlight):
        if parse_err is None:
            self.file_output_dict["parse_error"] = None
            self.file_output_dict["file_parse_error_start_context"] = None
            self.file_output_dict["file_parse_error_highlight"] = None
        else:
            self.file_output_dict["parse_error"] = parse_err[0:1000]
            self.file_output_dict["file_parse_error_start_context"] = parse_err_start[0:800]
            self.file_output_dict["file_parse_error_highlight"] = parse_err_highlight[0:500]

    def update_dict_parse_error_sqlglot(self, error):
        self.file_output_dict["parse_error"] = ''
        self.file_output_dict["file_parse_error_start_context"] = ''
        self.file_output_dict["file_parse_error_highlight"] = ''
        for idx, err in enumerate(error.errors):
            if idx > 0:
                if err['highlight'] != error.errors[idx-1]['highlight']:
                    self.file_output_dict["file_parse_error_start_context"] += err['start_context'][0:200] + '|'
                    self.file_output_dict["file_parse_error_highlight"] += err['highlight'][0:200] + '|'

                else:
                    self.file_output_dict["parse_error"] += err['description'][0:200] + '|'
            else:
                self.file_output_dict["parse_error"] += err['description'][0:200] + '|'
            
                self.file_output_dict["file_parse_error_start_context"] += err['start_context'][0:200] + '|'
                self.file_output_dict["file_parse_error_highlight"] += err['highlight'][0:200] + '|'

        self.file_output_dict["parse_error"] = self.file_output_dict["parse_error"][0:1000]
        self.file_output_dict["file_parse_error_start_context"] = self.file_output_dict["file_parse_error_start_context"][0:800]
        self.file_output_dict["file_parse_error_highlight"] = self.file_output_dict["file_parse_error_highlight"][0:500]

    def parse_file(self):
        
        self.file_output_dict["parsed"] = 0

        # trying to parse the whole file
        write_results = True
        for _dialect in self.mandatory_dialects:
            try:
                with timeout(10):
                    parsed_statements = sqlglot.parse(self.file_contents,read=_dialect,error_level=sqlglot.ErrorLevel.RAISE)
                    
                    # marking dialect
                    self.update_output_dict(_dialect,1)
                    self.file_output_dict["parsed"] = 1

                    if write_results is True:
                        self.file_output_dict["dialect"] = _dialect
                        self.file_output_dict["num_statements"] = len(parsed_statements)

                        self.file_output_dict["comment_count"] = self.get_comment_count(self.file_contents)

                        for stmt in parsed_statements:
                            
                            result = self.parse_one(stmt)

                            self.update_counter(stmt,self.cntr)
                            
                            for key, value in result.items():

                                if "num_" in key and "_stmt" in key:
                                    self.file_output_dict[key] += value
                                elif "num_ctr_" in key:
                                    self.file_output_dict[key] += value
                                elif "num_constraints" in key:
                                    self.file_output_dict[key] += value
                                elif "_list" in key:
                                    self.file_output_dict[key] += value

                        for key, value in self.file_output_dict.items():
                            if "_list" in key and "postgres_"  not in key:
                                self.file_output_dict[key] = list(set(self.file_output_dict[key]))
                        
                        self.file_output_dict['num_distinct_tables'] = len(self.file_output_dict['table_list'])
                        self.file_output_dict['num_distinct_columns'] = len(self.file_output_dict['column_list'])
                        self.file_output_dict['num_distinct_schemas'] = len(self.file_output_dict['schema_list'])
                        self.file_output_dict['num_distinct_dbs'] = len(self.file_output_dict['db_list'])
                        self.file_output_dict['num_view_stmt'] = len(self.file_output_dict['view_list'])

                        write_results = False

                    if _dialect == 'postgres':
                        # write postgres results anyway
                        self.file_output_dict["postgres_num_statements"] = len(parsed_statements)

                        self.file_output_dict["postgres_comment_count"] = self.get_comment_count(self.file_contents)

                        for stmt in parsed_statements:
                            result = self.parse_one(stmt)

                            self.update_counter(stmt,self.postgres_cntr)

                            for key, value in result.items():

                                if "num_" in key and "_stmt" in key:
                                    self.file_output_dict["postgres_" + key] += value
                                elif "num_ctr_" in key:
                                    self.file_output_dict["postgres_" + key] += value
                                elif "num_constraints" in key:
                                    self.file_output_dict["postgres_" + key] += value
                                elif "_list" in key:
                                    self.file_output_dict["postgres_" + key] += value

                        for key, value in self.file_output_dict.items():
                            if "_list" in key and "postgres_" in key:
                                self.file_output_dict[key] = list(set(self.file_output_dict[key]))
                        
                        self.file_output_dict['postgres_num_distinct_tables'] = len(self.file_output_dict['postgres_table_list'])
                        self.file_output_dict['postgres_num_distinct_columns'] = len(self.file_output_dict['postgres_column_list'])
                        self.file_output_dict['postgres_num_distinct_schemas'] = len(self.file_output_dict['postgres_schema_list'])
                        self.file_output_dict['postgres_num_distinct_dbs'] = len(self.file_output_dict['postgres_db_list'])
                        self.file_output_dict['postgres_num_view_stmt'] = len(self.file_output_dict['postgres_view_list'])     
                    
                    

            except TimeoutError as e:
                self.update_output_dict(_dialect,0)
                self.update_dict_parse_error('TIMEOUT_ERROR','','')
            except sqlglot.errors.ParseError as e:
                self.update_output_dict(_dialect,0)
                self.update_dict_parse_error_sqlglot(e)
            except Exception as e:
                self.update_output_dict(_dialect,0)
                self.update_dict_parse_error('NON_PARSER_ERROR|' + str(e),'','')

        # no mandatory dialect was successful
        if self.file_output_dict["parsed"] == 0:
            for _dialect in self.other_dialects:
                if self.file_output_dict["parsed"] == 1:
                    break
                try:
                    with timeout(7):
                        parsed_statements = sqlglot.parse(self.file_contents,read=_dialect,error_level=sqlglot.ErrorLevel.RAISE)
                        
                        # marking dialect
                        self.update_output_dict(_dialect,1)
                        self.file_output_dict["parsed"] = 1

                        if write_results is True:
                            #self.file_output_dict["dialect"] = _dialect
                            self.file_output_dict["num_statements"] = len(parsed_statements)

                            self.file_output_dict["comment_count"] = self.get_comment_count(self.file_contents)

                            for stmt in parsed_statements:
                                result = self.parse_one(stmt)
                                self.update_counter(stmt,self.cntr)
                                for key, value in result.items():

                                    if "num_" in key and "_stmt" in key:
                                        self.file_output_dict[key] += value
                                    elif "num_ctr_" in key:
                                        self.file_output_dict[key] += value
                                    elif "num_constraints" in key:
                                        self.file_output_dict[key] += value
                                    elif "_list" in key:
                                        self.file_output_dict[key] += value

                            for key, value in self.file_output_dict.items():
                                if "_list" in key and "postgres_"  not in key:
                                    self.file_output_dict[key] = list(set(self.file_output_dict[key]))
                            
                            self.file_output_dict['num_distinct_tables'] = len(self.file_output_dict['table_list'])
                            self.file_output_dict['num_distinct_columns'] = len(self.file_output_dict['column_list'])
                            self.file_output_dict['num_distinct_schemas'] = len(self.file_output_dict['schema_list'])
                            self.file_output_dict['num_distinct_dbs'] = len(self.file_output_dict['db_list'])
                            self.file_output_dict['num_view_stmt'] = len(self.file_output_dict['view_list'])
                
                except TimeoutError as e:
                    self.update_output_dict(_dialect,0)
                    self.update_dict_parse_error('TIMEOUT_ERROR','','')
                except sqlglot.errors.ParseError as e:
                    self.update_output_dict(_dialect,0)
                    self.update_dict_parse_error_sqlglot(e)
                except Exception as e:
                    self.update_output_dict(_dialect,0)
                    self.update_dict_parse_error('NON_PARSER_ERROR|' + str(e),'','')

        # cleaning errors
        if self.file_output_dict["parsed"] == 1:
            self.update_dict_parse_error(None,None,None)

        # statement level now

        _idx = 0

        statements = sqlparser.split(self.file_contents)

        if self.file_output_dict["parsed"] == 1:
            _stmt_dialects = [self.file_output_dict["dialect"]]
        else:
            _stmt_dialects = self.mandatory_dialects + self.other_dialects

        for statement in statements:

            _counter = Counter()
            _parsed = 0
            _err_stop = 0
            _result = {}

            for k in self.sqlglot_attributes:
                _result[k] = None

            for k in self.output_attributes:
                if '_list' in k:
                    _result[k] = []
                else:
                    _result[k] = 0

            _result['counter_str'] = None
            _result['postrgres_counter_str'] = None

            _result["statement_id"] = _idx
            _result['parsed'] = 0
            for _dialect in _stmt_dialects:
                if _parsed == 1 or _err_stop == 1:
                    break
                try:
                    with timeout(3):
                        parsed_statements = sqlglot.parse(statement,read=_dialect,error_level=sqlglot.ErrorLevel.RAISE)
                        
                        _parsed = 1
                        _result['parsed'] = 1
                        _result['dialect'] = _dialect

                        _result["num_statements"] = len(parsed_statements)

                        _result["comment_count"] = self.get_comment_count(statement)

                        for stmt in parsed_statements:
                            if stmt is not None:

                                result = self.parse_one(stmt)
                                self.update_counter(stmt,_counter)
                                
                                for key, value in result.items():

                                    if "num_" in key and "_stmt" in key:
                                        _result[key] += value
                                    elif "num_ctr_" in key:
                                        _result[key] += value
                                    elif "num_constraints" in key:
                                        _result[key] += value
                                    elif "_list" in key:
                                        _result[key] += value

                                for key, value in _result.items():
                                    if "_list" in key:
                                        _result[key] = list(set(_result[key]))
                                
                                _result['num_distinct_tables'] = len(_result['table_list'])
                                _result['num_distinct_columns'] = len(_result['column_list'])
                                _result['num_distinct_schemas'] = len(_result['schema_list'])
                                _result['num_distinct_dbs'] = len(_result['db_list'])
                                _result['num_view_stmt'] = len(_result['view_list'])

                except TimeoutError as e:
                    _result['parsed'] = 0
                    _result['parse_error'] = 'TIMEOUT_ERROR'
                    _result['file_parse_error_start_context'] = ''
                    _result['file_parse_error_highlight'] = ''
                    _err_stop = 1
                except sqlglot.errors.ParseError as e:
                    _result['parsed'] = 0

                    _result["parse_error"] = ''
                    _result["file_parse_error_start_context"] = ''
                    _result["file_parse_error_highlight"] = ''
                    for idx, err in enumerate(e.errors):
                        if idx > 0:
                            if err['highlight'] != e.errors[idx-1]['highlight']:
                                _result["file_parse_error_start_context"] += err['start_context'][0:200] + '|'
                                _result["file_parse_error_highlight"] += err['highlight'][0:200] + '|'

                            else:
                                _result["parse_error"] += err['description'][0:200] + '|'
                        else:
                            _result["parse_error"] += err['description'][0:200] + '|'
                        
                            _result["file_parse_error_start_context"] += err['start_context'][0:200] + '|'
                            _result["file_parse_error_highlight"] += err['highlight'][0:200] + '|'

                    _result["parse_error"] = _result["parse_error"][0:600]
                    _result["file_parse_error_start_context"] = _result["file_parse_error_start_context"][0:400]
                    _result["file_parse_error_highlight"] = _result["file_parse_error_highlight"][0:200]

                except Exception as e:
                    _result['parsed'] = 0
                    _result['parse_error'] = 'NON_PARSER_ERROR|' + str(e)[0:300]
                    _result['file_parse_error_start_context'] = ''
                    _result['file_parse_error_highlight'] = ''
                    _err_stop = 1

            if _result['parsed'] == 1:
                _result['parse_error'] = None
                _result['file_parse_error_start_context'] = None
                _result['file_parse_error_highlight'] = None

            _result['original'] = statement
            _result['counter_str'] = self.get_ctr_str(_counter)
            self.results.append(_result)
            _idx += 1

    def get_flat_data(self):
        data = {
            'file_id': self.file_id, # string
            'file_path': self.file_path, # string
            'errors_at_read': self.err_replace # int 
        }

        for key, value in self.file_output_dict.items():
            if "_list" not in key:
                data[key] = value
            else:
                if len(value) == 0:
                    data[key] = None
                else:
                    data[key] = value

        data['counter_str'] = self.get_ctr_str(self.cntr)
        data['postrgres_counter_str'] = self.get_ctr_str(self.postgres_cntr)

        statement_list = []
        for item in self.results:

            linked_data = {
                'file_id': self.file_id # string
            }

            for key, value in item.items():
                if "_list" not in key:
                    linked_data[key] = value
                else:
                    if len(value) == 0:
                        linked_data[key] = None
                    else:
                        linked_data[key] = value
            
            statement_list.append(linked_data)
        
        return self.OUTPATH, data, statement_list