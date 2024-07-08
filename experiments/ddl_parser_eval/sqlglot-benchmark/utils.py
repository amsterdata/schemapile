import uuid
import json
import pyarrow as pa
import pyarrow.parquet as pq
import os

class SQLParserSchema:
    def __init__(self):
        possible_types = ['UNKNOWN','DROP', 'CREATE', 'ALTER', 'INSERT', 'OTHER_DML', 'REPLACE']

        types_list = [pa.field("statement_type_" + item, pa.int64()) for item in possible_types]

        possible_tokens = ['Token.Assignment', 'Token.Comment', 'Token.Comment.Multiline', 'Token.Comment.Single', 'Token.Comment.Single.Hint', 
                    'Token.Error', 'Token.Keyword', 'Token.Keyword.CTE', 'Token.Keyword.DDL', 'Token.Keyword.DML', 'Token.Keyword.Order', 
                    'Token.Literal.Number.Float', 'Token.Literal.Number.Integer', 'Token.Literal.String.Single', 'Token.Literal.String.Symbol', 
                    'Token.Name', 'Token.Name.Builtin', 'Token.Name.Placeholder', 'Token.Operator', 'Token.Operator.Comparison', 
                    'Token.Punctuation', 'Token.Text.Whitespace.Newline', 'Token.Wildcard', 'Token.Generic.Command', 'Token.Generic', 
                    'Token.Text', 'Token.Text.Whitespace', 'Token.Other', 'Token.Literal','Token.Comment.Multiline.Hint',
                    'Token.Literal.Number.Hexadecimal','Token.Keyword.TZCast'
                ]

        struct_list = []
        for item in possible_tokens:
            if "Token.Literal.Number" in item or "Token.Literal.String" in item or "Token.Comment" in item:
                struct_list.append(pa.field(item,pa.list_(pa.int64())))
            else:
                struct_list.append(pa.field(item,pa.list_(pa.string())))

        self.statement_list_sch = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('statement_id', pa.string()),
            pa.field('statement_nr', pa.int64()),
            pa.field('statement_type', pa.string()),
            pa.field('has_comments', pa.int64()),
            pa.field('original', pa.string())
            ] + struct_list
        )

        self.file_level_schema = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('file_path', pa.string()),
            pa.field('errors_at_read',pa.int64()),
            pa.field('num_statements', pa.int64()),
            pa.field('file_has_comments', pa.int64()),
            pa.field('unknown_in_file', pa.int64())
            ] + types_list + struct_list
        )

class PGLastSchema:
    def __init__(self):
        self.statement_list_sch = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('statement_nr', pa.int64()),
            pa.field('num_statements', pa.int64()),
            pa.field('parsed', pa.int64()),
            pa.field('parse_error', pa.string()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('columns_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('counter_str', pa.string()),
            pa.field('original', pa.string()),
            pa.field('original_highlight', pa.string()),
            pa.field('comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64())
            ]
        )

        self.file_level_schema = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('file_path', pa.string()),
            pa.field('errors_at_read',pa.int64()),
            pa.field('num_statements', pa.int64()),
            pa.field('parsed_file', pa.int64()),
            pa.field('file_parse_error', pa.string()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('columns_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('counter_str', pa.string()),
            pa.field('original', pa.string()),
            pa.field('original_highlight', pa.string()),
            pa.field('file_comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64())
            ]
        )

class SQLGlotSchema:
    def __init__(self):
        self.statement_list_sch = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('statement_id',pa.int64()),
            pa.field('parsed',pa.int64()),
            pa.field('parsed_none',pa.int64()),
            pa.field('parsed_postgres',pa.int64()),
            pa.field('parsed_mysql',pa.int64()),
            pa.field('parsed_tsql',pa.int64()),
            pa.field('dialect',pa.string()),
            pa.field('parse_error',pa.string()),
            pa.field('file_parse_error_start_context',pa.string()),
            pa.field('file_parse_error_highlight',pa.string()),
            pa.field('num_statements', pa.int64()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('column_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64()),
            pa.field('num_command_stmt', pa.int64()),
            pa.field('num_command_alter_stmt', pa.int64()),
            pa.field('counter_str', pa.string()),
            pa.field('original',pa.string())
            ]
        )

        self.file_level_schema = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('file_path', pa.string()),
            pa.field('errors_at_read',pa.int64()),
            pa.field('parsed',pa.int64()),
            pa.field('parsed_none',pa.int64()),
            pa.field('parsed_postgres',pa.int64()),
            pa.field('parsed_mysql',pa.int64()),
            pa.field('parsed_tsql',pa.int64()),
            pa.field('dialect',pa.string()),
            pa.field('parse_error',pa.string()),
            pa.field('file_parse_error_start_context',pa.string()),
            pa.field('file_parse_error_highlight',pa.string()),
            pa.field('num_statements', pa.int64()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('column_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64()),
            pa.field('num_command_stmt', pa.int64()),
            pa.field('num_command_alter_stmt', pa.int64()),
            pa.field('counter_str', pa.string())
            ]
        )
        _new_sch = self.file_level_schema
        for f in self.file_level_schema:
            if "num" in f.name or "list" in f.name or f.name == 'comment_count' or f.name == 'counter_str':
                _new_sch  = _new_sch.append(pa.field('postgres_' + f.name, f.type))
        self.file_level_schema = _new_sch

class SimpleDDLParserSchema:
    def __init__(self):
        self.statement_list_sch = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('statement_nr', pa.int64()),
            pa.field('num_statements', pa.int64()),
            pa.field('parsed', pa.int64()),
            pa.field('parse_error', pa.string()),
            pa.field('value_error_present', pa.int64()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('column_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('original', pa.string()),
            pa.field('comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64())
            ]
        )

        self.file_level_schema = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('file_path', pa.string()),
            pa.field('errors_at_read',pa.int64()),
            pa.field('num_statements', pa.int64()),
            pa.field('parsed_file', pa.int64()),
            pa.field('file_parse_error', pa.string()),
            pa.field('value_error_present', pa.int64()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('column_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('file_comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64())
            ]
        )

class SQLFluffSchema:
    def __init__(self):

        self.statement_list_sch = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('statement_id', pa.string()),
            pa.field('statement_nr', pa.int64()),
            pa.field('successful_dialect', pa.string()),
            pa.field('parsed', pa.int64()),
            pa.field('parse_error', pa.string()),
            pa.field('parsed_num_errors', pa.int64()),
            pa.field('parse_error_pos', pa.string()),
            pa.field('raw_parse_result', pa.string()),
            pa.field('original', pa.string())
            ]
        )

        self.file_level_schema = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('file_path', pa.string()),
            pa.field('errors_at_read',pa.int64()),
            pa.field('num_statements', pa.int64()),
            pa.field('parsed_file', pa.int64()),
            pa.field('file_parse_error', pa.string()),
            pa.field('num_parsed_statements', pa.int64()),
            pa.field('file_num_errors', pa.int64()),
            pa.field('found_dialects', pa.string())
            ]
        )

class TIDBMysqlSchema:
    def __init__(self):
        self.statement_list_sch = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('statement_nr', pa.int64()),
            pa.field('num_statements', pa.int64()),
            pa.field('parsed', pa.int64()),
            pa.field('parse_error', pa.string()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('column_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('counter_str', pa.string()),
            pa.field('original', pa.string()),
            pa.field('original_highlight', pa.string()),
            pa.field('comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64())
            ]
        )

        self.file_level_schema = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('file_path', pa.string()),
            pa.field('errors_at_read',pa.int64()),
            pa.field('num_statements', pa.int64()),
            pa.field('parsed_file', pa.int64()),
            pa.field('file_parse_error', pa.string()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('column_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('counter_str', pa.string()),
            pa.field('original', pa.string()),
            pa.field('original_highlight', pa.string()),
            pa.field('file_comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64())
            ]
        )

class JSqlParserSchema:
    def __init__(self):
        self.statement_list_sch = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('statement_nr', pa.int64()),
            pa.field('num_statements', pa.int64()),
            pa.field('parsed', pa.int64()),
            pa.field('parse_error', pa.string()),
            pa.field('parse_error_with_brackets', pa.string()),
            pa.field('valid_ansi', pa.int64()),
            pa.field('valid_oracle', pa.int64()),
            pa.field('valid_mysql', pa.int64()),
            pa.field('valid_postgres', pa.int64()),
            pa.field('valid_sqlserver', pa.int64()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('column_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('counter_str', pa.string()),
            pa.field('original', pa.string()),
            pa.field('original_highlight', pa.string()),
            pa.field('comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64())
            ]
        )

        self.file_level_schema = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('file_path', pa.string()),
            pa.field('errors_at_read',pa.int64()),
            pa.field('num_statements', pa.int64()),
            pa.field('parsed_file', pa.int64()),
            pa.field('file_parse_error', pa.string()),
            pa.field('file_parse_error_with_brackets', pa.string()),
            pa.field('valid_ansi', pa.int64()),
            pa.field('valid_oracle', pa.int64()),
            pa.field('valid_mysql', pa.int64()),
            pa.field('valid_postgres', pa.int64()),
            pa.field('valid_sqlserver', pa.int64()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('column_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('counter_str', pa.string()),
            pa.field('original', pa.string()),
            pa.field('original_highlight', pa.string()),
            pa.field('file_comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64())
            ]
        )

class RustParserSchema:
    def __init__(self):
        self.statement_list_sch = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('statement_id',pa.int64()),
            pa.field('parsed',pa.int64()),
            pa.field('parsed_generic',pa.int64()),
            pa.field('parsed_ansi',pa.int64()),
            pa.field('parsed_postgres',pa.int64()),
            pa.field('parsed_mysql',pa.int64()),
            pa.field('parsed_ms',pa.int64()),
            pa.field('dialect',pa.string()),
            pa.field('parse_error',pa.string()),
            pa.field('num_statements', pa.int64()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('column_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64()),
            pa.field('num_command_stmt', pa.int64()),
            pa.field('num_command_alter_stmt', pa.int64()),
            pa.field('counter_str', pa.string()),
            pa.field('original',pa.string())
            ]
        )

        self.file_level_schema = pa.schema([
            pa.field('file_id', pa.string()),
            pa.field('file_path', pa.string()),
            pa.field('errors_at_read',pa.int64()),
            pa.field('parsed',pa.int64()),
            pa.field('parsed_generic',pa.int64()),
            pa.field('parsed_ansi',pa.int64()),
            pa.field('parsed_postgres',pa.int64()),
            pa.field('parsed_mysql',pa.int64()),
            pa.field('parsed_ms',pa.int64()),
            pa.field('dialect',pa.string()),
            pa.field('parse_error',pa.string()),
            pa.field('num_statements', pa.int64()),
            pa.field('num_distinct_tables', pa.int64()),
            pa.field('table_list', pa.list_(pa.string())),
            pa.field('num_distinct_columns', pa.int64()),
            pa.field('column_list', pa.list_(pa.string())),
            pa.field('num_distinct_schemas', pa.int64()),
            pa.field('schema_list', pa.list_(pa.string())),
            pa.field('num_distinct_dbs', pa.int64()),
            pa.field('db_list', pa.list_(pa.string())),
            pa.field('view_list', pa.list_(pa.string())),
            pa.field('num_constraints', pa.int64()),
            pa.field('num_ctr_notnull', pa.int64()),
            pa.field('num_ctr_unique', pa.int64()),
            pa.field('num_ctr_primary', pa.int64()),
            pa.field('num_ctr_foreign', pa.int64()),
            pa.field('comment_count', pa.int64()),
            pa.field('num_drop_stmt', pa.int64()),
            pa.field('num_drop_like_stmt', pa.int64()),
            pa.field('num_create_stmt', pa.int64()),
            pa.field('num_create_like_stmt', pa.int64()),
            pa.field('num_insert_stmt', pa.int64()),
            pa.field('num_insert_like_stmt', pa.int64()),
            pa.field('num_alter_stmt', pa.int64()),
            pa.field('num_alter_like_stmt', pa.int64()),
            pa.field('num_select_stmt', pa.int64()),
            pa.field('num_view_stmt', pa.int64()),
            pa.field('num_truncate_stmt', pa.int64()),
            pa.field('num_update_stmt', pa.int64()),
            pa.field('num_comment_stmt', pa.int64()),
            pa.field('num_delete_stmt', pa.int64()),
            pa.field('num_set_like_stmt', pa.int64()),
            pa.field('num_execute_like_stmt', pa.int64()),
            pa.field('num_index_like_stmt', pa.int64()),
            pa.field('num_transaction_like_stmt', pa.int64()),
            pa.field('num_command_stmt', pa.int64()),
            pa.field('num_command_alter_stmt', pa.int64()),
            pa.field('counter_str', pa.string())
            ]
        )
        _new_sch = self.file_level_schema
        for f in self.file_level_schema:
            if "num" in f.name or "list" in f.name or f.name == 'comment_count' or f.name == 'counter_str':
                _new_sch  = _new_sch.append(pa.field('postgres_' + f.name, f.type))
        self.file_level_schema = _new_sch

sqlparser = SQLParserSchema()
pglast = PGLastSchema()
sqlglot = SQLGlotSchema()
simpleddlparser = SimpleDDLParserSchema()
sqlfluff = SQLFluffSchema()
tidb_mysql = TIDBMysqlSchema()
jsqlparser = JSqlParserSchema()
sqlparser_rs = RustParserSchema()

def get_file_encodings(outdir):
    """
    outdir is expected to be './out_new/filedetails/'
    """

    encoding_dict = {}

    for file in os.listdir(outdir):
        full_filename = "%s/%s" % (outdir, file)
        with open(full_filename,'r') as fi:
            dict = json.load(fi)
            for item in dict:
                if item["encoding"] is not None:
                    encoding_dict[item['file_id']] = item['encoding']
                else:
                    encoding_dict[item['file_id']] ="utf-8"

    return encoding_dict


def write_to_json(outdir, data):
    """
    data is list of jsons
    """
    out_file_path = outdir + str(uuid.uuid4()) + '.json'

    with open(out_file_path, 'w+', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        #json.dump(data, f, ensure_ascii=False, indent=None, separators=(',', ':'))

def write_to_parquet(outdir, data, type=None, parser=None):

    if type == 'file_level':
        if parser is None:
            _schema = sqlparser.file_level_schema
        elif parser == 'PGLAST':
            _schema = pglast.file_level_schema
        elif parser == 'SQLGLOT':
            _schema = sqlglot.file_level_schema
        elif parser == 'SIMPLEDDLPARSER':
            _schema = simpleddlparser.file_level_schema
        elif parser == 'SQLFLUFF':
            _schema = sqlfluff.file_level_schema
        elif parser == 'TIDB_MYSQL':
            _schema = tidb_mysql.file_level_schema
        elif parser == 'JSQLPARSER':
            _schema = jsqlparser.file_level_schema
        elif parser == 'SQLPARSER_RS':
            _schema = sqlparser_rs.file_level_schema
    elif type == 'statement_level':
        if parser is None:
            _schema = sqlparser.statement_list_sch
        elif parser == 'PGLAST':
            _schema = pglast.statement_list_sch
        elif parser == 'SQLGLOT':
            _schema = sqlglot.statement_list_sch
        elif parser == 'SIMPLEDDLPARSER':
            _schema = simpleddlparser.statement_list_sch
        elif parser == 'SQLFLUFF':
            _schema = sqlfluff.statement_list_sch
        elif parser == 'TIDB_MYSQL':
            _schema = tidb_mysql.statement_list_sch
        elif parser == 'JSQLPARSER':
            _schema = jsqlparser.statement_list_sch
        elif parser == 'SQLPARSER_RS':
            _schema = sqlparser_rs.statement_list_sch
    else:
        _schema = None

    out_file_path = outdir + str(uuid.uuid4()) + '.parquet'

    #_data_json = json.loads(data)
    _table = pa.Table.from_pylist(data,schema=_schema)

    with pq.ParquetWriter(out_file_path, schema=_schema) as writer:
        writer.write_table(_table)