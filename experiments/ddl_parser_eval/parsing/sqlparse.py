import sqlparse as parser
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Text
#import hashlib
from src.FileHandler import FileHandler
from difflib import get_close_matches

class SQLParser(FileHandler):
    def __init__(self, file_path, encoding):
        """
        reads file contents and closes it
        """

        # inherit file_path, file_id
        super().__init__(file_path)

        self.OUTPATH = './out/sqlparser/'

        self.num_statements = None
        self.file_tokens = None
        self.file_has_comments = 0
        self.results = []

        self.unknown_in_file = 0

        self.possible_types = ['UNKNOWN','DROP', 'CREATE', 'ALTER', 'INSERT', 'OTHER_DML', 'REPLACE']

        self.file_statement_types = dict.fromkeys(self.possible_types,0)

        self.possible_tokens = ['Token.Assignment', 'Token.Comment', 'Token.Comment.Multiline', 'Token.Comment.Single', 'Token.Comment.Single.Hint', 
            'Token.Error', 'Token.Keyword', 'Token.Keyword.CTE', 'Token.Keyword.DDL', 'Token.Keyword.DML', 'Token.Keyword.Order', 
            'Token.Literal.Number.Float', 'Token.Literal.Number.Integer', 'Token.Literal.String.Single', 'Token.Literal.String.Symbol', 
            'Token.Name', 'Token.Name.Builtin', 'Token.Name.Placeholder', 'Token.Operator', 'Token.Operator.Comparison', 
            'Token.Punctuation', 'Token.Text.Whitespace.Newline', 'Token.Wildcard', 'Token.Generic.Command', 'Token.Generic', 
            'Token.Text', 'Token.Text.Whitespace', 'Token.Other', 'Token.Literal','Token.Comment.Multiline.Hint',
            'Token.Literal.Number.Hexadecimal','Token.Keyword.TZCast'
        ]


        super().file_open(file_encoding=encoding)
        super().file_read()
        super().file_close()

    def get_statement_id(self, statement):
        return str(hash(statement))
        #return hashlib.md5(parser.format(statement).strip().encode()).hexdigest()
        
    def parse_file(self):
        statements = parser.split(self.file_contents)

        self.num_statements = len(statements)
        
        _idx = 0
        _file_tokens = dict.fromkeys(self.possible_tokens,[])
        
        for statement in statements:
            _has_comments = 0
            _result = {
                'statement_id': None,
                'statement_nr': None,
                'statement_type': None,
                'has_comments': None,
                'tokens': dict.fromkeys(self.possible_tokens,[]),
                'original': None
                }

            _result['statement_id'] = self.get_statement_id(statement)
            _result['statement_nr'] = _idx
            _idx += 1

            _result['original'] = parser.format(statement)

            parsed_statement = parser.parse(statement)[0]
            _result['statement_type'] = parsed_statement.get_type()
            if _result['statement_type'] == 'UNKNOWN':
                self.unknown_in_file = 1

            if _result['statement_type'] in self.file_statement_types:
                self.file_statement_types[_result['statement_type']] += 1
            else:
                self.file_statement_types['OTHER_DML'] += 1

            _found_types = dict.fromkeys(self.possible_tokens,[])
            
            for token in parsed_statement.tokens:
                for t in token.flatten():
                    if t.ttype is not Text.Whitespace:
                        # TODO temporary debugging
                        if str(t.ttype) not in self.possible_tokens:
                            closest_key = get_close_matches(str(t.ttype),self.possible_tokens)
                            _found_types[closest_key] = _found_types[closest_key] + [t.value]
                            print(str(t.ttype))
                        else:
                            _found_types[str(t.ttype)] = _found_types[str(t.ttype)] + [t.value]

            for key, value in _found_types.items():
                _found_types[key] = list(dict.fromkeys(value))

                # saving space in output jsons by not keeping whole literals
                # and keeping number of distinct literals available (for int/float)
                # or number of chars (not distinct) in strings/comments
                if "Token.Literal.Number" in key:
                    _found_types[key] = [len(_found_types[key])]
                elif "Token.Literal.String" in key:
                    _found_types[key] = [len("".join(_found_types[key]))]
                elif "Token.Comment" in key:
                    _found_types[key] = [len("".join(_found_types[key]))]
                    _has_comments = 1

                if key not in _file_tokens:
                    # TODO temporary debugging
                    print(key)
                else:
                    _file_tokens[key] = _file_tokens[key] + _found_types[key]
                
            _result['has_comments'] = _has_comments
            if _has_comments == 1:
                self.file_has_comments = 1

            _result['tokens'] = _found_types
            self.results.append(_result)
        
        for key, value in _file_tokens.items():
            _file_tokens[key] = list(dict.fromkeys(value))
            if "Token.Literal.Number" in key:
                _file_tokens[key] = [sum(_file_tokens[key])]
            elif "Token.Literal.String" in key:
                _file_tokens[key] = [sum(_file_tokens[key])]
            elif "Token.Comment" in key:
                _file_tokens[key] = [sum(_file_tokens[key])]    
        self.file_tokens = _file_tokens
        

    def output(self):
        data = {
            'file_id': self.file_id,
            'file_path': self.file_path,
            'errors_at_read': self.err_replace,
            'num_statements': self.num_statements,
            'file_tokens': self.file_tokens,
            'file_has_comments': self.file_has_comments,
            'unknown_in_file': self.unknown_in_file,
            'types_in_file': self.file_statement_types,
            'statement_list': self.results
        }
        super().write_to_json(self.OUTPATH, data)

    def get_data(self):
        data = {
            'file_id': self.file_id,
            'file_path': self.file_path,
            'errors_at_read': self.err_replace,
            'num_statements': self.num_statements,
            'file_tokens': self.file_tokens,
            'file_has_comments': self.file_has_comments,
            'unknown_in_file': self.unknown_in_file,
            'types_in_file': self.file_statement_types,
            'statement_list': self.results
        }
        return self.OUTPATH, data

    def get_flat_data(self):
        data = {
            'file_id': self.file_id,
            'file_path': self.file_path,
            'errors_at_read': self.err_replace,
            'num_statements': self.num_statements,
            'file_has_comments': self.file_has_comments,
            'unknown_in_file': self.unknown_in_file
        }
        for key, value in self.file_statement_types.items():
            data["statement_type_" + key]= value
        for key, value in self.file_tokens.items():
            data[key] = value

        statement_list = []
        for item in self.results:

            linked_data = {
                'file_id': self.file_id,
                'statement_id': item['statement_id'],
                'statement_nr': item['statement_nr'],
                'statement_type': item['statement_type'],
                'has_comments': item['has_comments'],
                'original': item['original']
            }

            for key, value in item['tokens'].items():
                linked_data[key] = value

            statement_list.append(linked_data)

        return self.OUTPATH, data, statement_list
        