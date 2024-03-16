import sqlparse as sqlparser
from .file_handler import FileHandler
from src.timeout import timeout
from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JNetworkError,Py4JError
import re
from py4j.java_gateway import GatewayParameters

class JSqlParser(FileHandler):
    def __init__(self, file_path, encoding):
        """
        reads file contents and closes it
        """

        # inherit file_path, file_id
        super().__init__(file_path)

        # calling java
        self.gateway = JavaGateway(gateway_parameters=GatewayParameters(read_timeout=10))     

        self.OUTPATH = './out_new/tidb_mysql/'
        self.regex = re.compile('[^a-zA-Z]')

        self.num_statements = None
        self.parsed_file = None
        self.file_parse_error = None
        self.file_parse_error_with_brackets = None

        self.tidb_output_attrs = [
            'parsed_file', 'file_parse_error', 'file_parse_error_with_brackets', 'valid_ansi', 'valid_oracle', 'valid_mysql',
            'valid_postgres','valid_sqlserver',
            'counter_str', 'original', 'original_highlight', 'file_comment_count'
        ]

        self.output_attributes = [
            'num_statements','num_distinct_tables', 'num_distinct_columns', 'num_distinct_schemas', 'num_distinct_dbs',
            'table_list','column_list', 'db_list', 'schema_list', 'view_list', 'num_constraints', 'num_ctr_notnull',
            'num_ctr_unique', 'num_ctr_primary','num_ctr_foreign', 'num_drop_stmt', 'num_drop_like_stmt',
            'num_create_stmt', 'num_create_like_stmt','num_insert_stmt', 'num_insert_like_stmt', 'num_alter_stmt', 
            'num_alter_like_stmt', 'num_select_stmt', 'num_view_stmt','num_truncate_stmt', 'num_update_stmt', 
            'num_comment_stmt', 'num_delete_stmt', 'num_set_like_stmt', 'num_execute_like_stmt','num_index_like_stmt', 
            'num_transaction_like_stmt'
            ]

        self.file_output_dict = {}

        for k in self.tidb_output_attrs:
            self.file_output_dict[k] = None
        
        for k in self.output_attributes:
            if '_list' in k:
                self.file_output_dict[k] = []
            else:
                self.file_output_dict[k] = 0

        self.results = []

        super().file_open(file_encoding=encoding)
        super().file_read()
        super().file_close()

    def parse_successful_result(self, result_string):

        split_result = result_string.split(';|;')
        
        try:
            parsed_overall = int(split_result[0])
            _result = {
                "num_statements": int(split_result[1]), 
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
                "valid_ansi": int(split_result[2]),
                "valid_oracle": int(split_result[3]),
                "valid_mysql": int(split_result[4]),
                "valid_postgres": int(split_result[5]),
                "valid_sqlserver": int(split_result[6]),
                "num_constraints": None, 
                "num_ctr_notnull": int(split_result[12]), 
                "num_ctr_unique": int(split_result[13]), 
                "num_ctr_primary": int(split_result[14]), 
                "num_ctr_foreign": int(split_result[15]), 
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
                "num_transaction_like_stmt": None
        }
        except Exception as e:
            print(e)
            print(self.file_id)
        
        cl_str = split_result[8]
        tl_str = split_result[7]
        sch_str = split_result[9]
        db_str = split_result[10]
        vw_str = split_result[11]

        _result['table_list'] = [item.upper() for item in tl_str.split(';')]
        _result['table_list'] = list(set(_result['table_list']))
        _result['table_list'] = list(filter(None, _result['table_list']))
        _result['num_distinct_tables'] = len(_result['table_list'])

        _result['column_list'] = [item.upper() for item in cl_str.split(';')]
        _result['column_list'] = list(set(_result['column_list']))
        _result['column_list'] = list(filter(None, _result['column_list']))
        _result['num_distinct_columns'] = len(_result['column_list'])

        _result['db_list'] = [item.upper() for item in db_str.split(';')]
        _result['db_list'] = list(set(_result['db_list']))
        _result['db_list'] = list(filter(None, _result['db_list']))
        _result['num_distinct_dbs'] = len(_result['db_list'])

        _result['schema_list'] = [item.upper() for item in sch_str.split(';')]
        _result['schema_list'] = list(set(_result['schema_list']))
        _result['schema_list'] = list(filter(None, _result['schema_list']))
        _result['num_distinct_schemas'] = len(_result['schema_list'])

        _result['view_list'] = [item.upper() for item in vw_str.split(';')]
        _result['view_list'] = list(set(_result['view_list']))
        _result['view_list'] = list(filter(None, _result['view_list']))

        return parsed_overall,_result
    
    def parse_one(self, stmt, id):
        _result = {
            "statement_nr": id,
            "parsed": None, 
            "parse_error": None,
            'parse_error_with_brackets': None,
            "original": None,
            "original_highlight": None
        }

        parsed_timeout = False
        try:
            for i in range(0,5):
                if not parsed_timeout:
                    try:
                        obj = self.gateway.entry_point.getParserObj(stmt)
                        parsed_str = obj.parse()
                        parsed_err_tried_without_brackets = obj.get_err_tried_without_brackets()
                        parsed_err_tried_with_brackets = obj.get_err_tried_with_brackets()
                        p, go_results = self.parse_successful_result(parsed_str)
                        if p == 0:
                            _result['parsed'] = 0
                            _result['parse_error'] = parsed_err_tried_without_brackets
                            _result['parse_error_with_brackets'] = parsed_err_tried_with_brackets
                        else:
                            _result['parsed'] = 1
                            _result['parse_error'] = parsed_err_tried_without_brackets
                            _result['parse_error_with_brackets'] = parsed_err_tried_with_brackets
                            __result = go_results
                            for k,v in __result.items():
                                _result[k] = v
                        parsed_timeout = True
                    except Py4JError as e:
                        print("TIMEOUT - try: " + str(i))
                        print(self.file_id)
                        self.file_output_dict['parsed'] = 0
                        self.file_output_dict['parse_error'] = 'CONNECTION_ERROR|' + str(e)[0:500]
        except Exception as e:
            print("##################")
            print(self.file_id)
            print(e)
            _result['parsed'] = 0
            _result['parse_error'] = 'OTHER_ERROR|' + str(e)[0:400]
        return _result
    
    def parse_file(self):
        parsed_timeout = False

        # max retries
        try:
            for i in range(0,5):
                if not parsed_timeout:
                    try:
                        obj = self.gateway.entry_point.getParserObj(self.file_contents)
                        parsed_str = obj.parse()
                        parsed_err_tried_without_brackets = obj.get_err_tried_without_brackets()
                        parsed_err_tried_with_brackets = obj.get_err_tried_with_brackets()
                        p, go_results = self.parse_successful_result(parsed_str)
                        if p == 0:
                            self.file_output_dict['parsed_file'] = 0
                            if len(parsed_err_tried_without_brackets) == 0:
                                self.file_output_dict['file_parse_error'] = None
                            else:
                                self.file_output_dict['file_parse_error'] = parsed_err_tried_without_brackets
                            if len(parsed_err_tried_with_brackets) == 0:
                                self.file_output_dict['file_parse_error_with_brackets'] = None
                            else:
                                self.file_output_dict['file_parse_error_with_brackets'] = parsed_err_tried_with_brackets
                        else:
                            self.file_output_dict['parsed_file'] = 1
                            if len(parsed_err_tried_without_brackets) == 0:
                                self.file_output_dict['file_parse_error'] = None
                            else:
                                self.file_output_dict['file_parse_error'] = parsed_err_tried_without_brackets
                            if len(parsed_err_tried_with_brackets) == 0:
                                self.file_output_dict['file_parse_error_with_brackets'] = None
                            else:
                                self.file_output_dict['file_parse_error_with_brackets'] = parsed_err_tried_with_brackets
                            # go through each field and populate file
                                    
                            for k,v in go_results.items():
                                if v is not None:
                                    self.file_output_dict[k] = v
                        parsed_timeout = True
                    except Py4JError as e:
                        print("TIMEOUT - try: " + str(i))
                        print(self.file_id)
                        self.file_output_dict['parsed_file'] = 0
                        self.file_output_dict['file_parse_error'] = 'CONNECTION_ERROR|' + str(e)[0:500]
        except Exception as e:
            print("##################")
            print(self.file_id)
            print(e)
            self.file_output_dict['parsed_file'] = 0
            self.file_output_dict['file_parse_error'] = 'OTHER_ERROR|' + str(e)[0:500]

        # stmt level

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
        }

        for key, value in self.file_output_dict.items():
            if "_list" not in key:
                data[key] = value
            else:
                if len(value) == 0:
                    data[key] = None
                else:
                    data[key] = value

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
