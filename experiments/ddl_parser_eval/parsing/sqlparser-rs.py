import sqloxide
from itertools import chain
import sqlparse as sqlparser
from .file_handler import FileHandler
from .timeout import timeout
import re

class RustParser(FileHandler):
    def __init__(self, file_path, encoding):
        """
        reads file contents and closes it
        """

        # inherit file_path, file_id
        super().__init__(file_path)

        
        self.OUTPATH = './out_new/rustparser/'
        self.regex = re.compile('[^a-zA-Z]')

        self.mandatory_dialects = ["generic", "ansi","postgres","mysql","ms"]
        self.other_dialects = [
            "sqlite", "snowflake", "redshift", "hive", "bigquery", "clickhouse"
        ]

        self.sqlglot_attributes = [
            'parsed', 
            'parsed_generic', 'parsed_ansi', 'parsed_postgres', 'parsed_mysql', 'parsed_ms', 
            'dialect', 
            'parse_error',
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

    def gen_dict_extract(self,key, var):
        if hasattr(var,'items'): 
            for k, v in var.items() :
                if k == key:
                    yield v
                if isinstance(v, dict):
                    for result in self.gen_dict_extract(key, v):
                        yield result
                elif isinstance(v, list):
                    for d in v:
                        for result in self.gen_dict_extract(key, d):
                            yield result
        elif isinstance(var,list):
            for v in var:
                for result in self.gen_dict_extract(key,v):
                    yield result

    def get_col_defs(self, d):
        cols_list = []
        if "CreateTable" in d:
            cols = d["CreateTable"]["columns"]
            for col in cols:
                cols_list.append(col["name"]["value"])
        return cols_list

    def get_constraints(self, d):
        pk = 0
        uk = 0
        fk = 0
        nn = 0

        for item in self.gen_dict_extract('Unique',d):
            if 'is_primary' in item:
                if item['is_primary'] is True:
                    pk += 1
                else:
                    uk += 1
        for item in self.gen_dict_extract("columns",d):
            if isinstance(item,list):
                for col in item:
                    if "options" in col:
                        for opt in col["options"]:
                            if not isinstance(opt['option'],dict):
                                if opt['option'] == 'NotNull':
                                    nn += 1

        for item in self.gen_dict_extract("AlterColumn",d):
            if item["op"] == "SetNotNull":
                nn += 1

        for item in self.gen_dict_extract("ChangeColumn", d):
            for opt in item["options"]:
                if not isinstance(opt,dict):
                    if opt == 'NotNull':
                        nn += 1         
        
        try:
            for item in self.gen_dict_extract("AddColumn", d):
                for opt in item["column_def"]["options"]:
                    if not isinstance(opt['option'],dict):
                        if opt['option'] == 'NotNull':
                            nn += 1
        except KeyError:
            pass

        for item in self.gen_dict_extract("ForeignKey", d):
            fk += 1
            
        return nn,pk,uk,fk

    def get_ident_from_key(self, d,key):
        """
        {'name': [{'value': 'test_sch', 'quote_style': None}, {'value': 'TEST_PLM', 'quote_style': None}], 'alias': None, 'args': None, 'with_hints': []}

        {'table_name': [{'value': 'test_tr_sch', 'quote_style': None}, {'value': 'test_truncate', 'quote_style': None}]
            => [{'value': 'test_tr_sch', 'quote_style': None}, {'value': 'test_truncate', 'quote_style': None}]
        """
        db = None
        sch = None
        tbl = None
        


        if key is None:
            obj = d
        else:
            if key in d:
                obj = d[key]
            else:
                return None,None,None

        if isinstance(obj,dict):
            if 'Table' in obj:
                obj = obj['Table']['name']
        elif isinstance(obj,list):
            if len(obj) == 3:
                db = obj[0]["value"]
                sch = obj[1]["value"]
                tbl = obj[2]["value"]
            elif len(obj) == 2:
                sch = obj[0]["value"]
                tbl = obj[1]["value"]
            elif len(obj) == 1:
                tbl = obj[0]["value"]
        elif isinstance(obj,str):
            tbl = obj

        return db,sch,tbl

    def get_d_s_t_v(self, d):

        dbs = []
        schs = []
        tbls = []
        vws = []

        if "Drop" in d:
            """
            {'Drop': {'object_type': 'Table', 'if_exists': True, 'names': [[{'value': 'sch', 'quote_style': None}, {'value': 'CUSTOMER', 'quote_style': None}]], 'cascade': True, 'restrict': False, 'purge': False}}
            """
            if d["Drop"]["object_type"] == 'Table':
                for item in self.gen_dict_extract('names',d["Drop"]):
                    _db,_sch,_tbl = self.get_ident_from_key(list(chain.from_iterable(item)),None)
                    dbs.append(_db)
                    schs.append(_sch)
                    tbls.append(_tbl)
            elif d["Drop"]["object_type"] == 'View':
                for item in self.gen_dict_extract('names',d["Drop"]):
                    _db,_sch,_tbl = self.get_ident_from_key(list(chain.from_iterable(item)),None)
                    dbs.append(_db)
                    schs.append(_sch)
                    vws.append(_tbl)

        for item in self.gen_dict_extract('Table',d):
            _db,_sch,_tbl = self.get_ident_from_key(item,"name")
            dbs.append(_db)
            schs.append(_sch)
            tbls.append(_tbl)

        if "CreateTable" in d:
            _db,_sch,_tbl = self.get_ident_from_key(d["CreateTable"],"name")
            dbs.append(_db)
            schs.append(_sch)
            tbls.append(_tbl)

        """
        {'Truncate': {'table_name': [{'value': 'test_tr_sch', 'quote_style': None}, {'value': 'test_truncate', 'quote_style': None}], 'partitions': None}}
        """
        for item in self.gen_dict_extract('table_name',d):
            _db,_sch,_tbl = self.get_ident_from_key(item,None)
            dbs.append(_db)
            schs.append(_sch)
            tbls.append(_tbl)

        """
        'foreign_table': [{'value': 'reviewable', 'quote_style': None}]
        """
        for item in self.gen_dict_extract('foreign_table',d):
            _db,_sch,_tbl = self.get_ident_from_key(item,None)
            dbs.append(_db)
            schs.append(_sch)
            tbls.append(_tbl)

        """
        {'CreateSchema': {'schema_name': {'NamedAuthorization': ([{'value': 'blaine', 'quote_style': None}], {'value': 'dba', 'quote_style': None})}, 'if_not_exists': False}}
        {'CreateSchema': {'schema_name': {'Simple': [{'value': 'blaine_simple', 'quote_style': None}]}, 'if_not_exists': False}}
        CREATE SCHEMA blaine_simple;
        CREATE SCHEMA blaine AUTHORIZATION dba;
        """

        for item in self.gen_dict_extract('schema_name',d):
            if item is not None:
                if "NamedAuthorization" in item:
                    if len(item["NamedAuthorization"][0]) == 1:
                        schs.append(item["NamedAuthorization"][0][0]["value"])
                    elif len(item["NamedAuthorization"][0]) == 2:
                        dbs.append(item["NamedAuthorization"][0][0]["value"])
                        schs.append(item["NamedAuthorization"][0][1]["value"])
                elif "Simple" in item:
                    if len(item["Simple"]) == 1 :
                        schs.append(item["Simple"][0]["value"])
                    elif len(item["Simple"]) == 2:
                        dbs.append(item["Simple"][0]["value"])
                        schs.append(item["Simple"][1]["value"])
        
        for item in self.gen_dict_extract('db_name',d):
            if isinstance(item,list):
                dbs.append(item[0]["value"])
            elif isinstance(item,dict):
                dbs.append(item['value'])


        """
        {'Comment': {'object_type': 'Table', 'object_name': [{'value': 'x', 'quote_style': None}], 'comment': 'bla', 'if_exists': False}}
        """
        if "Comment" in d:
            if d["Comment"]["object_type"] == 'Table':
                _db,_sch,_tbl = self.get_ident_from_key(d["Comment"],"object_name")
                dbs.append(_db)
                schs.append(_sch)
                tbls.append(_tbl)

        """
        {'AlterTable': {'name': [{'value': 'sch', 'quote_style': None}, {'value': 'BLA', 'quote_style': None}], 'operation': {'DropColumn': {'column_name': {'value': 'ABA', 'quote_style': None}, 'if_exists': False, 'cascade': False}}}}
        """
        if "AlterTable" in d:
            _db,_sch,_tbl = self.get_ident_from_key(d["AlterTable"],"name")
            dbs.append(_db)
            schs.append(_sch)
            tbls.append(_tbl)

        if "CreateView" in d:
            _db,_sch,_vw = self.get_ident_from_key(d["CreateView"],"name")
            dbs.append(_db)
            schs.append(_sch)
            vws.append(_vw)
        
        return dbs,schs,tbls,vws
    
    def parse_one(self, stmt, id=None):
        """
        stmt is json after parsing
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

        _result['db_list'], _result['schema_list'], _result['table_list'], _result['view_list'] = self.get_d_s_t_v(stmt)

        _result['table_list'] = list(filter(None, _result['table_list']))
        _result['table_list'] = [item.upper() for item in _result['table_list']]
        _result['table_list'] = list(set(_result['table_list']))
        _result['num_distinct_tables'] = len(_result['table_list'])

        _result['view_list'] = list(filter(None, _result['view_list']))
        _result['view_list'] = [item.upper() for item in _result['view_list']]
        _result['view_list'] = list(set(_result['view_list']))
        _result['num_view_stmt'] = len(_result['view_list'])

        _result['schema_list'] = list(filter(None, _result['schema_list']))
        _result['schema_list'] = [item.upper() for item in _result['schema_list']]
        _result['schema_list'] = list(set(_result['schema_list']))
        _result['num_distinct_schemas'] = len(_result['schema_list'])

        _result['db_list'] = list(filter(None, _result['db_list']))
        _result['db_list'] = [item.upper() for item in _result['db_list']]
        _result['db_list'] = list(set(_result['db_list']))
        _result['num_distinct_dbs'] = len(_result['db_list'])

        _result['column_list'] = self.get_col_defs(stmt)
        _result['column_list'] = list(filter(None, _result['column_list']))
        _result['column_list'] = [item.upper() for item in _result['column_list']]
        _result['column_list'] = list(set(_result['column_list']))
        _result['num_distinct_columns'] = len(_result['column_list'])

        # nn,pk,uk,fk

        _result['num_ctr_notnull'], _result['num_ctr_primary'], _result['num_ctr_unique'], _result['num_ctr_foreign'] = self.get_constraints(stmt)
        
        return _result
    
    def update_output_dict(self, dialect, parse_result):
        if dialect in self.mandatory_dialects:
            self.file_output_dict["parsed_" + dialect] = parse_result
        else:
            if parse_result == 1:
                self.file_output_dict["dialect"] = dialect

    def update_dict_parse_error(self, parse_err):
        if parse_err is None:
            self.file_output_dict["parse_error"] = None
        else:
            self.file_output_dict["parse_error"] = parse_err[0:1000]

    def parse_file(self):
        
        self.file_output_dict["parsed"] = 0

        # trying to parse the whole file
        write_results = True
        for _dialect in self.mandatory_dialects:
            
            try:
                with timeout(10):
                    parsed_statements = sqloxide.parse_sql(sql=self.file_contents,dialect=_dialect)
                    
                    # marking dialect
                    self.update_output_dict(_dialect,1)
                    self.file_output_dict["parsed"] = 1

                    if write_results is True:
                        self.file_output_dict["dialect"] = _dialect
                        self.file_output_dict["num_statements"] = len(parsed_statements)

                        self.file_output_dict["comment_count"] = None

                        for stmt in parsed_statements:
                            
                            result = self.parse_one(stmt)
                            
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

                        self.file_output_dict["postgres_comment_count"] = None

                        for stmt in parsed_statements:
                            result = self.parse_one(stmt)

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
                self.update_dict_parse_error('TIMEOUT_ERROR')
            except ValueError as e:
                self.update_output_dict(_dialect,0)
                self.update_dict_parse_error('PARSER_ERROR|' + str(e))
            except KeyError as e:
                print("==========KEY_ERR=======")
                print(self.file_id)
                print(_dialect)
                print(e)
            except IndexError as e:
                print("==========IDX_ERR=======")
                print(self.file_id)
                print(_dialect)
                print(e)
            except Exception as e:
                print("=============EXC===============")
                print(self.file_id)
                print(_dialect)
                print(e)
                self.update_output_dict(_dialect,0)
                self.update_dict_parse_error('NON_PARSER_ERROR|' + str(e))

        # no mandatory dialect was successful
        if self.file_output_dict["parsed"] == 0:
            for _dialect in self.other_dialects:
                if self.file_output_dict["parsed"] == 1:
                    break
                try:
                    with timeout(7):
                        parsed_statements = sqloxide.parse_sql(sql=self.file_contents,dialect=_dialect)
                        
                        # marking dialect
                        self.update_output_dict(_dialect,1)
                        self.file_output_dict["parsed"] = 1

                        if write_results is True:
                            #self.file_output_dict["dialect"] = _dialect
                            self.file_output_dict["num_statements"] = len(parsed_statements)

                            self.file_output_dict["comment_count"] = None

                            for stmt in parsed_statements:
                                result = self.parse_one(stmt)
                                
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
                    self.update_dict_parse_error('TIMEOUT_ERROR')
                except ValueError as e:
                    self.update_output_dict(_dialect,0)
                    self.update_dict_parse_error('PARSER_ERROR|' + str(e))
                except KeyError as e:
                    print("==========KEY_ERR=======")
                    print(self.file_id)
                    print(_dialect)
                    print(e)
                except IndexError as e:
                    print("==========IDX_ERR=======")
                    print(self.file_id)
                    print(_dialect)
                    print(e)
                except Exception as e:
                    print("================EXC============")
                    print(e)
                    print(_dialect)
                    print(self.file_id)
                    self.update_output_dict(_dialect,0)
                    self.update_dict_parse_error('NON_PARSER_ERROR|' + str(e))

        # cleaning errors
        if self.file_output_dict["parsed"] == 1:
            self.update_dict_parse_error(None)

        # statement level now
        _idx = 0

        statements = sqlparser.split(self.file_contents)

        if self.file_output_dict["parsed"] == 1:
            _stmt_dialects = [self.file_output_dict["dialect"]]
        else:
            _stmt_dialects = self.mandatory_dialects + self.other_dialects

        for statement in statements:

            
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
                        parsed_statements = sqloxide.parse_sql(sql=statement,dialect=_dialect)
                        
                        _parsed = 1
                        _result['parsed'] = 1
                        _result['dialect'] = _dialect

                        _result["num_statements"] = len(parsed_statements)

                        _result["comment_count"] = None

                        for stmt in parsed_statements:
                            if stmt is not None:

                                result = self.parse_one(stmt)
                                
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
                    _err_stop = 1
                except ValueError as e:
                    _result['parsed'] = 0
                    _result["parse_error"] = str(e)[0:1000]
                except KeyError as e:
                    print("==========STMT - KEY_ERR=======")
                    print(self.file_id)
                    print(statement)
                    print(_dialect)
                    print(e)
                except IndexError as e:
                    print("==========STMT - IDX_ERR=======")
                    print(self.file_id)
                    print(statement)
                    print(_dialect)
                    print(e)
                except Exception as e:
                    print("==========STMT - EXC=======")
                    print(self.file_id)
                    print(e)
                    _result['parsed'] = 0
                    _result['parse_error'] = 'NON_PARSER_ERROR|' + str(e)[0:300]
                    _err_stop = 1

            if _result['parsed'] == 1:
                _result['parse_error'] = None
                _result['file_parse_error_start_context'] = None
                _result['file_parse_error_highlight'] = None

            _result['original'] = statement
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

        data['counter_str'] = None
        data['postrgres_counter_str'] = None

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