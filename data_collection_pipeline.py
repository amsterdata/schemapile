import gzip
import json
import os
import sys
import random
import time
import hashlib
import shutil
from chardet.universaldetector import UniversalDetector
import sqloxide
import sqlparse
import datetime
import requests
import threading
import tqdm
from multiprocessing import Pool
from sqloxide import restore_ast

sqlfiles_path = "data/sqlfiles/"
quarantine = "data/sqlfiles_duplicates/"
sqlasts_path = "data/sqlasts/"
urls_path = "data/urls_and_licenses.json.gz"
schemapile_path = "data/schemapile.json"

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15"
]
n_download_threads = 10
n_parsing_threads = 10

dialects = ["generic", "ansi", "postgres", "mysql", "ms", "sqlite", "snowflake", "redshift", "hive", "bigquery", "clickhouse"]


# parse files, mutithreaded
def file_encoding(file_path):
    sql_file = open(file_path, 'rb')
    detector = UniversalDetector()
    detector.reset()
    for line in sql_file:
        detector.feed(line)
        if detector.done: break
    detector.close()

    return detector.result['encoding']

def file_read(file_path):
    try:
        encoding = file_encoding(file_path)
    except:
        encoding = "utf-8"

    try:
        sql_file = open(file_path,'r',encoding=encoding)
    except ValueError:
        sql_file = open(file_path,'r',encoding=encoding, errors='replace')

    try:
        file_contents = sql_file.read()
    except Exception as e:
        sql_file = open(file_path,'rb')
        file_contents = sql_file.read().decode(errors='replace')

    return file_contents

# define the function that does the actual work
def process_file(file):
    if os.path.exists(sqlasts_path+file) or os.path.isdir(sqlfiles_path+file):
        return


    file_content = file_read(sqlfiles_path + file)
    statements = sqlparse.split(file_content)

    asts = []
    for statement in statements:
        for dialect in dialects:
            try:
                parsed_statements = sqloxide.parse_sql(sql=statement,dialect=dialect)
                asts.extend(parsed_statements)
                break
            except:
                continue
    if len(asts) > 0:
        with open(sqlasts_path+file, "w+") as f:
            json.dump(asts, f)


if __name__ ==  '__main__':
    first_n = -1
    if len(sys.argv) > 1:
        first_n = int(sys.argv[1])

    # open urls
    with gzip.open(urls_path, 'r') as f:
        urls_and_licenses = json.loads(f.read().decode('utf-8'))

    print(f"total urls: {len(urls_and_licenses)}")

    # take subset
    urls_and_licenses = {key: urls_and_licenses[key] for key in list(urls_and_licenses.keys())[:first_n]}

    # download files
    def download_file(filename, url):
        try:
            time.sleep(random.random()/10)
            headers = {"User-Agent": random.choice(user_agents)}
            response = requests.get(url, allow_redirects=True, headers=headers)
            with open(sqlfiles_path+filename, "wb") as file:
                file.write(response.content)
        except Exception as e:
            print(e)
            return

    def download_files(urls_and_licenses):
        start = time.time()
        existing = 0
        downloaded = 0

        urls_and_licenses_filtered = []
        for filename in urls_and_licenses:
            if os.path.exists(sqlfiles_path+filename):
                existing += 1
                continue
            urls_and_licenses_filtered.append((filename,urls_and_licenses[filename]["INFO"]["URL"]))

        url_batches = [urls_and_licenses_filtered[i:i+n_download_threads] for i in range(0, len(urls_and_licenses_filtered), n_download_threads)]
        for url_batch in url_batches:
            # Generate a list of threads for this batch
            threads = [threading.Thread(target = download_file, args = (url[0],url[1])) for url in url_batch]

            # Start the batch of threads
            for thread in threads:
                thread.start()

            # Wait for all threads in this batch to finish
            for thread in threads:
                thread.join()

            downloaded += n_download_threads
            if (downloaded) % 10 == 0:
                current_time = time.time()
                remaining = (current_time - start) / (downloaded+1) * (len(urls_and_licenses) - (downloaded+existing))
                remaining_time = str(datetime.timedelta(seconds=remaining))
                print(f"{downloaded+existing}/{len(urls_and_licenses)} downloaded (estimated remaining time {remaining_time})", end="\r")

    os.makedirs(sqlfiles_path, exist_ok=True)
    download_files(urls_and_licenses)
    print("downloaded all files                                                     ")

    # deduplicate Files
    def chunk_reader(fobj, chunk_size=1024):
        """Generator that reads a file in chunks of bytes"""
        while True:
            chunk = fobj.read(chunk_size)
            if not chunk:
                return
            yield chunk

    os.makedirs(quarantine, exist_ok=True)
    duplicates = []
    hashes = {}
    i = 0
    j = 0
    for dirpath, dirnames, filenames in os.walk(sqlfiles_path):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            hashobj = hashlib.sha256()
            for chunk in chunk_reader(open(full_path, 'rb')):
                hashobj.update(chunk)
            file_id = (hashobj.digest(), os.path.getsize(full_path))
            duplicate = hashes.get(file_id, None)
            if duplicate:
                duplicates.append(full_path)
                j += 1
                shutil.move(full_path, quarantine+os.path.basename(full_path))
            else:
                hashes[file_id] = full_path

            if i % 10000 == 0:
                print(f"\r{j} duplicates found, after {i} files", end='\r')
            i += 1
    print(f"{j} duplicates found, after {i} files")


    # split and Parse SQL Files
    os.makedirs(sqlasts_path, exist_ok=True)
    sqlfiles = os.listdir(sqlfiles_path)

    # create a multiprocessing pool
    pool = Pool(n_parsing_threads)

    # map the function to the list of files in a parallel manner
    print("start parsing files...")
    for _ in tqdm.tqdm(pool.imap_unordered(process_file, sqlfiles), total=len(sqlfiles)):
        pass

    # don't forget to close the pool after use
    pool.close()
    pool.join()
    print("finished parsing files.")

    # extract and transform to SchemaPile
    def get_table_name(table_statement):
        # return only table name
        # return table_statement[-1]["value"]

        # return fully qualified name
        return ".".join([name_statement["value"] for name_statement in table_statement])

    def change_quote_style_dict(dct):
        for key in dct:
            if key == "quote_style":
                dct[key] = None
            if type(dct[key]) is list:
                dct[key] = change_quote_style_list(dct[key])
            if type(dct[key]) is dict:
                dct[key] = change_quote_style_dict(dct[key])
        return dct

    def change_quote_style_list(lst):
        for i, el in enumerate(lst):
            if type(el) is dict:
                lst[i] = change_quote_style_dict(el)
            if type(el) is list:
                lst[i] = change_quote_style_list(el)
        return lst

    def get_sql_expression_from_check(check):
        dummy_check_wrapper = [{'CreateTable': {'or_replace': False,
          'temporary': False,
          'external': False,
          'global': None,
          'if_not_exists': False,
          'transient': False,
          'name': [{'value': 'DUMMY_TABLE', 'quote_style': None}],
          'columns': [{'name': {'value': 'DUMMY', 'quote_style': None},
            'data_type': {'Int': None},
            'collation': None,
            'options': [{'name': None,
              'option': {'Check': check}}]}],
          'constraints': [],
          'hive_distribution': 'NONE',
          'hive_formats': {'row_format': None, 'storage': None, 'location': None},
          'table_properties': [],
          'with_options': [],
          'file_format': None,
          'location': None,
          'query': None,
          'without_rowid': False,
          'like': None,
          'clone': None,
          'engine': None,
          'default_charset': None,
          'collation': None,
          'on_commit': None,
          'on_cluster': None,
          'order_by': None,
          'strict': False}}]

        dummy_check_wrapper_single_quotes = change_quote_style_list(dummy_check_wrapper)
        query = restore_ast(ast=dummy_check_wrapper_single_quotes)
        return query[0][43:-2]

    def get_existing_case_insensitive(elem, lst):
        for existing_elem in lst:
            if elem.lower() == existing_elem.lower():
                return existing_elem

    def extract_and_add_constraints(constraint_statement, table):
        if "Unique" in constraint_statement:
            if constraint_statement["Unique"]["is_primary"]:
                if len(table["PRIMARY_KEYS"]) > 0:
                    for column in table["PRIMARY_KEYS"]:
                        table["COLUMNS"][column]["IS_PRIMARY"] = False
                        table["COLUMNS"][column]["NULLABLE"] = None
                        table["COLUMNS"][column]["UNIQUE"] = None
                    table["PRIMARY_KEYS"] = []
                for primary_key in [col["value"] for col in constraint_statement["Unique"]["columns"]]:
                    if existing_column:= get_existing_case_insensitive(primary_key, table["COLUMNS"].keys()):
                        if len(constraint_statement["Unique"]["columns"]) == 1:
                            table["COLUMNS"][existing_column]["UNIQUE"] = True
                        table["COLUMNS"][existing_column]["IS_PRIMARY"] = True
                        table["COLUMNS"][existing_column]["NULLABLE"] = False
                        if existing_column not in table["PRIMARY_KEYS"]:
                            table["PRIMARY_KEYS"].append(existing_column)
            else:
                for unique_col in [col["value"] for col in constraint_statement["Unique"]["columns"]]:
                    if existing_column:= get_existing_case_insensitive(unique_col, table["COLUMNS"].keys()):
                        table["COLUMNS"][existing_column]["UNIQUE"] = True
        elif "ForeignKey" in constraint_statement:
            foreign_key_foreign_table = get_table_name(constraint_statement["ForeignKey"]["foreign_table"])
            foreign_key_source_columns = [col["value"] for col in constraint_statement["ForeignKey"]["columns"]]
            foreign_key_referred_columns = [col["value"] for col in constraint_statement["ForeignKey"]["referred_columns"]]
            if len(foreign_key_source_columns) > 0 and len(foreign_key_source_columns) == len(foreign_key_referred_columns):
                foreign_key = {"COLUMNS": foreign_key_source_columns, "FOREIGN_TABLE": foreign_key_foreign_table, "REFERRED_COLUMNS": foreign_key_referred_columns, "ON_DELETE": constraint_statement["ForeignKey"]["on_delete"], "ON_UPDATE": constraint_statement["ForeignKey"]["on_update"]}
                if foreign_key not in table["FOREIGN_KEYS"]:
                    table["FOREIGN_KEYS"].append(foreign_key)
        elif "Check" in constraint_statement:
            check_statement = constraint_statement["Check"]
            check = get_sql_expression_from_check(check_statement["expr"])
            if check not in table["CHECKS"]:
                table["CHECKS"].append(check)
        elif "Index" in constraint_statement:
            index_statement = constraint_statement["Index"]
            index_columns = []
            for index_column in [col["value"] for col in index_statement["columns"]]:
                if existing_column:= get_existing_case_insensitive(index_column, table["COLUMNS"].keys()):
                    table["COLUMNS"][existing_column]["IS_INDEX"] = True
                    index_columns.append(existing_column)
            table["INDEXES"].append(index_columns)

    def extract_schema(schema_json):
        tables = {}

        # extract schema and constraint from create table statements
        for statement_statement in schema_json["statements"]:
            if "CreateTable" in statement_statement:
                create_table_statement = statement_statement["CreateTable"]
                table_name = get_table_name(create_table_statement["name"])
                columns_statement = create_table_statement["columns"]
                columns = {}
                primary_keys = []
                foreign_keys = []
                for column_statement in columns_statement:
                    column_name = column_statement["name"]["value"]
                    if column_name in ["INDEX", "PRIMARY KEY", "KEY"] and column_statement["name"]["quote_style"] is None:
                        continue  # this is constraint that was not parsed correctly, we discard it
                    column_type_statement = column_statement["data_type"]
                    if isinstance(column_type_statement, str):
                        column_type = column_type_statement
                    elif type(column_type_statement) is dict and "Custom" in column_type_statement:
                        column_type = column_type_statement["Custom"][0][0]["value"]
                    elif type(column_type_statement) is dict and len(column_type_statement) > 0:
                        column_type = list(column_type_statement.keys())[0]
                    else:
                        raise Exception(f"Column type not found in statement: {column_type_statement}")
                    options_statement = column_statement["options"]
                    nullable = None
                    unique = None
                    primary_key = False
                    foreign_key = None
                    default = None
                    checks = []
                    for option_statement in options_statement:
                        if isinstance(option_statement["option"], str) and option_statement["option"] == "NotNull":
                            nullable = False
                        elif isinstance(option_statement["option"], str) and option_statement["option"] == "Null":
                            nullable = True
                        elif type(option_statement["option"]) is dict and "Unique" in option_statement["option"]:
                            unique = True
                            primary_key = option_statement["option"]["Unique"]["is_primary"]
                            if primary_key:
                                nullable = False
                        elif (type(option_statement["option"]) is dict and "Default" in option_statement["option"]
                            and "Value" in option_statement["option"]["Default"] and "SingleQuotedString" in option_statement["option"]["Default"]["Value"]):
                            default = option_statement["option"]["Default"]["Value"]["SingleQuotedString"]
                        elif type(option_statement["option"]) is dict and "Check" in option_statement["option"]:
                            check_statement = option_statement["option"]["Check"]
                            check = get_sql_expression_from_check(check_statement)
                            checks.append(check)
                        elif type(option_statement["option"]) is dict and "ForeignKey" in option_statement["option"]:
                            if (len(option_statement["option"]["ForeignKey"]["referred_columns"]) == 1):
                                foreign_key_foreign_table = get_table_name(option_statement["option"]["ForeignKey"]["foreign_table"])
                                foreign_key_referred_column = option_statement["option"]["ForeignKey"]["referred_columns"][0]["value"]
                                foreign_key = {"COLUMNS": [column_name], "FOREIGN_TABLE": foreign_key_foreign_table, "REFERRED_COLUMNS": [foreign_key_referred_column], "ON_DELETE": option_statement["option"]["ForeignKey"]["on_delete"], "ON_UPDATE": option_statement["option"]["ForeignKey"]["on_update"]}
                    if primary_key:
                        primary_keys.append(column_name)
                    if foreign_key:
                        foreign_keys.append(foreign_key)
                    columns[column_name] = {"TYPE": column_type, "NULLABLE": nullable, "UNIQUE": unique, "DEFAULT": default, "CHECKS": checks, "IS_PRIMARY": primary_key, "IS_INDEX": False}
                table = {"COLUMNS": columns, "PRIMARY_KEYS": primary_keys, "FOREIGN_KEYS": foreign_keys, "CHECKS": [], "INDEXES": []}
                tables[table_name] = table

                constraints_statement = create_table_statement["constraints"]
                if len(constraints_statement) > 0:
                    for constraint_statement in constraints_statement:
                        extract_and_add_constraints(constraint_statement, tables[table_name])

        # remove empty tables
        for table_name in list(tables.keys()):
            if len(tables[table_name]["COLUMNS"]) == 0:
                tables.pop(table_name)
        # don't add empty schemas
        if len(tables) == 0:
            return None

        # extract additional constraint from alter statements
        for statement_statement in schema_json["statements"]:
            if "AlterTable" in statement_statement:
                table_name = statement_statement["AlterTable"]["name"][-1]["value"]
                if existing_table:= get_existing_case_insensitive(table_name, tables.keys()):
                    if "AddConstraint" in statement_statement["AlterTable"]["operation"]:
                        add_constraint_statement = statement_statement["AlterTable"]["operation"]["AddConstraint"]
                        extract_and_add_constraints(add_constraint_statement, tables[existing_table])

        # filter and clean up foreign keys (keep only those that refer to existing columns/tables)
        for table_name in tables:
            foreign_keys_original = tables[table_name]["FOREIGN_KEYS"]
            foreign_keys_clean = []
            for foreign_key_original in foreign_keys_original:
                if foreign_key_foreign_table_exists:= get_existing_case_insensitive(foreign_key_original["FOREIGN_TABLE"], tables.keys()):
                    foreign_key_columns_exist = []
                    for column_original in foreign_key_original["COLUMNS"]:
                        if column_exists:= get_existing_case_insensitive(column_original, tables[table_name]["COLUMNS"].keys()):
                            foreign_key_columns_exist.append(column_exists)
                    foreign_key_referred_columns_exist = []
                    for referred_column_original in foreign_key_original["REFERRED_COLUMNS"]:
                        if referred_column_exists:= get_existing_case_insensitive(referred_column_original, tables[foreign_key_foreign_table_exists]["COLUMNS"].keys()):
                            foreign_key_referred_columns_exist.append(referred_column_exists)
                    if (len(foreign_key_columns_exist) == len(foreign_key_original["COLUMNS"])
                        and len(foreign_key_referred_columns_exist) == len(foreign_key_original["REFERRED_COLUMNS"])
                       and len(foreign_key_columns_exist) == len(foreign_key_referred_columns_exist)):
                        foreign_key_original["COLUMNS"] = foreign_key_columns_exist
                        foreign_key_original["FOREIGN_TABLE"] = foreign_key_foreign_table_exists
                        foreign_key_original["REFERRED_COLUMNS"] = foreign_key_referred_columns_exist
                        foreign_keys_clean.append(foreign_key_original)
            tables[table_name]["FOREIGN_KEYS"] = foreign_keys_clean

        schema = {"INFO": {"URL": schema_json["url"], "LICENSE": schema_json["license"], "PERMISSIVE": schema_json["permissive"]}, "TABLES": tables}
        return schema

    # extract schemas from sqlast files
    print("start extracting schemas from asts...")
    asts_files = sorted(os.listdir(sqlasts_path))
    schemapile = {}
    for i, schema_name in enumerate(asts_files):
        if not os.path.isfile(sqlasts_path+schema_name):
            continue
        if i % 1000 == 0:
            print(f"{i}/{len(asts_files)}", end="\r")
        with open(sqlasts_path+schema_name, "r") as f:
            statements = json.load(f)
            ast_json = []
            if schema_name not in urls_and_licenses:
                continue
            info = urls_and_licenses[schema_name]["INFO"]
            for statement in statements:
                ast_json.append(statement)
            schema = extract_schema({"url": info["URL"], "license": info["LICENSE"], "permissive": info["PERMISSIVE"], "statements": ast_json})
            if schema:
                schemapile[schema_name] = schema
    print(f"successfully extracted {len(schemapile)}/{len(asts_files)} schemas")


    # deduplicate extracted schemas
    print("deduplicating schemas...")
    unique_schemas = set()
    schemapile_dedup = {}
    for file in schemapile:
        schema_json = json.dumps(schemapile[file]["TABLES"])
        if schema_json not in unique_schemas:
            unique_schemas.add(schema_json)
            schemapile_dedup[file] = schemapile[file]

    print(f"deduplicated schemas: {len(schemapile)}/{len(schemapile_dedup)} (before/after)")
    schemapile = schemapile_dedup


    # add data content
    import ast
    def extract_value(vals):
        values = []
        for val in vals:
            if "UnaryOp" in val and val["UnaryOp"]["op"] == "Minus":
                values.append(-ast.literal_eval(val["UnaryOp"]['expr']['Value']['Number'][0]))
            elif "Identifier" in val and val["Identifier"]["value"] == "default":
                values.append("<default>")
            elif "Number" in val["Value"]:
                values.append(ast.literal_eval(val["Value"]["Number"][0]))
            elif "SingleQuotedByteStringLiteral" in val["Value"]:
                values.append(val["Value"]["SingleQuotedByteStringLiteral"])
            elif "HexStringLiteral" in val["Value"]:
                values.append(val["Value"]["HexStringLiteral"])
            elif "Boolean" in val["Value"]:
                values.append(val["Value"]["Boolean"])
            elif "SingleQuotedString" in val["Value"]:
                values.append(val["Value"]["SingleQuotedString"])
            elif "DoubleQuotedString" in val["Value"]:
                values.append(val["Value"]["DoubleQuotedString"])
            elif "Null" in val["Value"]:
                values.append(None)
            else:
                raise Exception("Unknown type: " + str(val["Value"]))
        return values

    inserts = 0
    schemas_w_insert = 0
    inserts_parsed = 0
    inserts_table_found = 0
    inserts_columns_match = 0
    inserts_columns_match_partial = 0
    print("start extracting data content...")
    for i, file in enumerate(schemapile):
        with open(sqlasts_path+file, "r") as f:
            asts = json.load(f)

        insert_found = False
        for astt in asts:
            if "Insert" in astt:
                inserts += 1
                insert_found = True
                try:
                    table_name = astt["Insert"]["table_name"][0]["value"]
                    columns = [col["value"] for col in astt["Insert"]["columns"]]
                    values = [extract_value(val) for val in astt["Insert"]["source"]["body"]["Values"]["rows"]]

                    if len(values) > 0:
                        inserts_parsed += 1
                    else:
                        continue

                    if table_name in schemapile[file]["TABLES"]:
                        inserts_table_found += 1
                        columns_schema = list(schemapile[file]["TABLES"][table_name]["COLUMNS"].keys())

                        if len(columns) == 0 and len(columns_schema) == len(values[0]):
                            inserts_columns_match += 1
                            columns = columns_schema
                        elif len(columns) == len(columns_schema) and all([column in set(columns_schema) for column in columns]):
                            inserts_columns_match += 1
                        elif len(columns) > 0 and all([column in set(columns_schema) for column in columns]):
                            inserts_columns_match_partial += 1
                        else:
                            continue

                        for column in columns:
                            schemapile[file]["TABLES"][table_name]["COLUMNS"][column]["VALUES"] = []

                        for row in values:
                            for column_idx, column in enumerate(columns):
                                value = row[column_idx]
                                schemapile[file]["TABLES"][table_name]["COLUMNS"][column]["VALUES"].append(value)

                except Exception as e:
                    continue
        if insert_found:
            schemas_w_insert += 1
        if i % 1000 == 0:
            print(f"{i}/{schemas_w_insert}/{inserts}/{inserts_parsed}/{inserts_table_found}/{inserts_columns_match}/{inserts_columns_match_partial} (total/schemas w inserts/inserts found/inserts_values_found/inserts_table_found/inserts_columns_match/inserts_columns_match_partial)", end="\r")

    print(f"{i}/{schemas_w_insert}/{inserts}/{inserts_parsed}/{inserts_table_found}/{inserts_columns_match}/{inserts_columns_match_partial} (total/schemas w inserts/inserts found/inserts_values_found/inserts_table_found/inserts_columns_match/inserts_columns_match_partial)")
    print("finished extracting data content.")


    # save SchemaPile
    print("saving SchemaPile...")
    with open(schemapile_path, 'w') as file:
        json.dump(schemapile, file)
    print(f"saved schemapile to {schemapile_path}")
