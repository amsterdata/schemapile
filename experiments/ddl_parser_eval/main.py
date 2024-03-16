import multiprocessing
import os
from pathlib import Path
import shutil
from parsing.file_handler import FileHandler
from parsing.sqlparser import SQLParser
import parsing.utils as ut
from datetime import datetime
import sys
from parsing.pglast import PGLast
from itertools import repeat
from parsing.sqlglot import SQLGlot
import time
from multiprocessing import set_start_method
from parsing.simple_ddl_parser import SimpleDDLParser
from parsing.jsqlparser import JSqlParser
from parsing.sqlparser_rs import RustParser

CHUNK_SIZE = 3000


OUTDIR = {
    'file': './out_new/filedetails/',
    'sqlparser': './out_new/sqlparser/',
    'sqlparser_details': './out_new/sqlparser_details/',
    'pglast': './out_new/pglast/',
    'pglast_details': './out_new/pglast_details/',
    'sqlglot': './out_new/sqlglot/',
    'sqlglot_details': './out_new/sqlglot_details/',
    'simpleddlparser': './out_new/simpleddlparser/',
    'jsqlparser': './out_new/jsqlparser/',
    'jsqlparser_details': './out_new/jsqlparser_details/',
    'sqlparser-rs': './out_new/rustparser/',
    'sqlparser-rs_details': './out_new/rustparser_details/',
    }

INDIR = '../sqlfiles/'

def process_sqlparser(file_path_list):
    results = {
        'file': [],
        'sqlparser': [],
        'sqlparser_details': []
    }

    for file_path in file_path_list:
        f = FileHandler(file_path)
        f.get_file_details()
        f_encoding, _ = f.get_encoding()
        #f.output()
        _, _f_result = f.get_data()
        results["file"].append(_f_result)

        s = SQLParser(file_path, f_encoding)
        s.parse_file()

        #s.output()
        _, _s_result, _s_details = s.get_flat_data()
        results["sqlparser"].append(_s_result)
        results['sqlparser_details'] += _s_details

    
    ut.write_to_json(OUTDIR['file'], results['file'])
    ut.write_to_parquet(OUTDIR['sqlparser'], results['sqlparser'], type='file_level')
    ut.write_to_parquet(OUTDIR['sqlparser_details'], results['sqlparser_details'], type='statement_level')
    #ut.write_to_json(OUTDIR['sqlparser'], results['sqlparser'])
    #ut.write_to_json(OUTDIR['sqlparser_details'], results['sqlparser_details'])

    del results

def process_pglast(file_path_list, file_encodings):
    results = {
        'pglast': [],
        'pglast_details': []
    }
    for file_path in file_path_list:
        p = PGLast(file_path,file_encodings[file_path.split('/')[-1].split('_')[0]])

        p.parse_file()
        _, _p_result, _p_details = p.get_flat_data()
        results["pglast"].append(_p_result)
        results['pglast_details'] += _p_details

    #ut.write_to_json(OUTDIR['pglast'], results['pglast'])
    #ut.write_to_json(OUTDIR['pglast_details'], results['pglast_details'])

    ut.write_to_parquet(OUTDIR['pglast'] + [s for s in multiprocessing.current_process().name if s.isdigit()][0] + '__',
                        results['pglast'], type='file_level',parser='PGLAST')
    ut.write_to_parquet(OUTDIR['pglast_details'] + [s for s in multiprocessing.current_process().name if s.isdigit()][0] + '__',
                        results['pglast_details'], type='statement_level',parser='PGLAST')
    del results

def process_sqlglot(file_path_list, file_encodings):
    print ('Starting', multiprocessing.current_process().name)
    results = {
        'sqlglot': [],
        'sqlglot_details': []
    }

    for file_path in file_path_list:
        s = SQLGlot(file_path,file_encodings[file_path.split('/')[-1].split('_')[0]])

        s.parse_file()
        _, _s_result, _s_details = s.get_flat_data()

        results["sqlglot"].append(_s_result)
        results['sqlglot_details'] += _s_details

    #ut.write_to_json(OUTDIR['sqlglot'], results['sqlglot'])
    #ut.write_to_json(OUTDIR['sqlglot_details'], results['sqlglot_details'])
    ut.write_to_parquet(OUTDIR['sqlglot'] + [s for s in multiprocessing.current_process().name if s.isdigit()][0] + '__', 
                        results['sqlglot'], type='file_level',parser='SQLGLOT')
    ut.write_to_parquet(OUTDIR['sqlglot_details'] + [s for s in multiprocessing.current_process().name if s.isdigit()][0] + '__',
                         results['sqlglot_details'], type='statement_level',parser='SQLGLOT')

    del results

    print ('Ending', multiprocessing.current_process().name)

def process_simpleddlparser(file_path_list, file_encodings):
    print ('Starting', multiprocessing.current_process().name)

    results = {
        'simpleddlparser': [],
        'simpleddlparser_details': []
    }

    for file_path in file_path_list:
        #print("Doing: " + file_path)
        s = SimpleDDLParser(file_path,file_encodings[file_path.split('/')[-1].split('_')[0]])

        s.parse_file()
        _, _s_result, _s_details = s.get_flat_data()

        results["simpleddlparser"].append(_s_result)
        results['simpleddlparser_details'] += _s_details

    #ut.write_to_json(OUTDIR['simpleddlparser'], results['simpleddlparser'])
    #ut.write_to_json(OUTDIR['simpleddlparser_details'], results['simpleddlparser_details'])
    ut.write_to_parquet(OUTDIR['simpleddlparser'] + [s for s in multiprocessing.current_process().name if s.isdigit()][0] + '__',
                         results['simpleddlparser'], type='file_level',parser='SIMPLEDDLPARSER')
    ut.write_to_parquet(OUTDIR['simpleddlparser_details'] + [s for s in multiprocessing.current_process().name if s.isdigit()][0] + '__',
                         results['simpleddlparser_details'], type='statement_level',parser='SIMPLEDDLPARSER')

    del results

    print ('Ending', multiprocessing.current_process().name)

def process_jsqlparser(file_path_list, file_encodings):
    print ('Starting', multiprocessing.current_process().name)
    
    results = {
        'jsqlparser': [],
        'jsqlparser_details': []
    }

    for file_path in file_path_list:
        #print("Doing: " + file_path)
        parse_start = datetime.now()

        s = JSqlParser(file_path,file_encodings[file_path.split('/')[-1].split('_')[0]])

        s.parse_file()
        _, _s_result, _s_details = s.get_flat_data()

        results["jsqlparser"].append(_s_result)
        results['jsqlparser_details'] += _s_details

        parse_end = datetime.now()

        #print("file_took|" + file_path + "|" + str(parse_end-parse_start))

    #ut.write_to_json(OUTDIR['jsqlparser'], results['jsqlparser'])
    #ut.write_to_json(OUTDIR['jsqlparser_details'], results['jsqlparser_details'])
    ut.write_to_parquet(OUTDIR['jsqlparser'] + [s for s in multiprocessing.current_process().name if s.isdigit()][0] + '__',
                         results['jsqlparser'], type='file_level',parser='JSQLPARSER')
    ut.write_to_parquet(OUTDIR['jsqlparser_details'] + [s for s in multiprocessing.current_process().name if s.isdigit()][0] + '__',
                         results['jsqlparser_details'], type='statement_level',parser='JSQLPARSER')

    del results

    print ('Ending', multiprocessing.current_process().name)

def process_sqlparser_rs(file_path_list, file_encodings):
    print ('Starting', multiprocessing.current_process().name)
    
    results = {
        'sqlparser-rs': [],
        'sqlparser-rs_details': []
    }

    for file_path in file_path_list:
        #print("Doing: " + file_path)
        parse_start = datetime.now()

        s = RustParser(file_path,file_encodings[file_path.split('/')[-1].split('_')[0]])

        s.parse_file()
        _, _s_result, _s_details = s.get_flat_data()

        results["sqlparser-rs"].append(_s_result)
        results['sqlparser-rs_details'] += _s_details

        parse_end = datetime.now()

    ut.write_to_parquet(OUTDIR['sqlparser-rs'] + [s for s in multiprocessing.current_process().name if s.isdigit()][0] + '__', results['rustparser'], type='file_level',parser='SQLPARSER_RS')
    ut.write_to_parquet(OUTDIR['sqlparser-rs_details'] + [s for s in multiprocessing.current_process().name if s.isdigit()][0] + '__', results['rustparser_details'], type='statement_level',parser='SQLPARSER_RS')

    del results

    print ('Ending', multiprocessing.current_process().name)

if __name__ == '__main__':

    args = sys.argv[1:]

    if args[0] == 'SQLPARSER':
        # remove outputs
        shutil.rmtree(OUTDIR['file'],ignore_errors=True)
        shutil.rmtree(OUTDIR['sqlparser_details'], ignore_errors=True)
        shutil.rmtree(OUTDIR['sqlparser'],ignore_errors=True)

        path = Path(OUTDIR['file'])
        path.mkdir(parents=True, exist_ok=True)

        path = Path(OUTDIR['sqlparser'])
        path.mkdir(parents=True, exist_ok=True)

        path = Path(OUTDIR['sqlparser_details'])
        path.mkdir(parents=True, exist_ok=True)
    elif args[0] == 'PGLAST':
        shutil.rmtree(OUTDIR['pglast_details'], ignore_errors=True)
        shutil.rmtree(OUTDIR['pglast'],ignore_errors=True)

        path = Path(OUTDIR['pglast'])
        path.mkdir(parents=True, exist_ok=True)

        path = Path(OUTDIR['pglast_details'])
        path.mkdir(parents=True, exist_ok=True)
    elif args[0] == 'SQLGLOT':
        shutil.rmtree(OUTDIR['sqlglot_details'], ignore_errors=True)
        shutil.rmtree(OUTDIR['sqlglot'],ignore_errors=True)

        path = Path(OUTDIR['sqlglot'])
        path.mkdir(parents=True, exist_ok=True)

        path = Path(OUTDIR['sqlglot_details'])
        path.mkdir(parents=True, exist_ok=True)    
    elif args[0] == 'SIMPLEDDLPARSER':
        shutil.rmtree(OUTDIR['simpleddlparser_details'], ignore_errors=True)
        shutil.rmtree(OUTDIR['simpleddlparser'],ignore_errors=True)

        path = Path(OUTDIR['simpleddlparser'])
        path.mkdir(parents=True, exist_ok=True)

        path = Path(OUTDIR['simpleddlparser_details'])
        path.mkdir(parents=True, exist_ok=True)    
    elif args[0] == 'JSQLPARSER':
        shutil.rmtree(OUTDIR['jsqlparser_details'], ignore_errors=True)
        shutil.rmtree(OUTDIR['jsqlparser'],ignore_errors=True)

        path = Path(OUTDIR['jsqlparser'])
        path.mkdir(parents=True, exist_ok=True)

        path = Path(OUTDIR['jsqlparser_details'])
        path.mkdir(parents=True, exist_ok=True)
    elif args[0] == 'SQLPARSER_RS':
        shutil.rmtree(OUTDIR['sqlparser-rs_details'], ignore_errors=True)
        shutil.rmtree(OUTDIR['sqlparser-rs'],ignore_errors=True)

        path = Path(OUTDIR['sqlparser-rs'])
        path.mkdir(parents=True, exist_ok=True)

        path = Path(OUTDIR['sqlparser-rs_details'])
        path.mkdir(parents=True, exist_ok=True)  

        sys.setrecursionlimit(1500)
    else:
        print("ERROR args")
        sys.exit(0)

    multiprocessing.set_start_method('spawn')
    p = multiprocessing.Pool()
    
    for dirpath, dirnames, filenames in os.walk(INDIR):
        file_list = [dirpath + file for file in sorted(filenames)]


    split_file_list = [file_list[x:x+CHUNK_SIZE] for x in range(0, len(file_list), CHUNK_SIZE)]

    if args[0] == 'SQLPARSER':
        p.map(process_sqlparser,split_file_list)
    elif args[0] == 'PGLAST':
        _file_encodings = ut.get_file_encodings(OUTDIR['file'])
        p.starmap(process_pglast, [(f_list, _file_encodings) for f_list in split_file_list])
    elif args[0] == 'SQLGLOT':
        _file_encodings = ut.get_file_encodings(OUTDIR['file'])
        p.starmap(process_sqlglot, [(f_list, _file_encodings) for f_list in split_file_list])
    elif args[0] == 'SIMPLEDDLPARSER':
        _file_encodings = ut.get_file_encodings(OUTDIR['file'])
        p.starmap(process_simpleddlparser, [(f_list, _file_encodings) for f_list in split_file_list])
    elif args[0] == 'JSQLPARSER':
        _file_encodings = ut.get_file_encodings(OUTDIR['file'])
        p.starmap(process_jsqlparser, [(f_list, _file_encodings) for f_list in split_file_list])
    elif args[0] == 'SQLPARSER_RS':
        _file_encodings = ut.get_file_encodings(OUTDIR['file'])
        p.starmap(process_sqlparser_rs, [(f_list, _file_encodings) for f_list in split_file_list])
    elif args[0] == 'SQLPARSER_RS_FOR_ERRORS':
        _file_encodings = ut.get_file_encodings(OUTDIR['file'])
        p.starmap(process_sqlparser_rs_for_errors, [(f_list, _file_encodings) for f_list in split_file_list])

    p.close()
    p.join()
