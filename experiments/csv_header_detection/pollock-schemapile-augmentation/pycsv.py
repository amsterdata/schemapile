import csv
from os import listdir
from os.path import abspath, join
from utils import print, save_time_df, load_parameters
import os
import time
import pickle
import numpy as np

sut = 'pycsv'
DATASET = os.environ['DATASET']
IN_DIR = abspath(f'/{DATASET}/csv/')
PARAM_DIR = abspath(f'/{DATASET}/parameters')
OUT_DIR = abspath(f'/results/{sut}/{DATASET}/loading/')
TIME_DIR = abspath(f'/results/{sut}/{DATASET}/')
N_REPETITIONS = 3

TO_SKIP = []

times_dict = {}
benchmark_files = listdir(IN_DIR)

column_names_schemapile = pickle.load(open("column_names_schemapile.pickle","rb"))
def outliers_distance_to_next_order(data):
    sorted_indices = list(np.argsort(data))
    if len(sorted_indices) >2:
        # Check if the maximum value is significantly larger than the mean of the other values
        if sorted_indices[-1]+1 < len(sorted_indices) and data[sorted_indices[-1]] > 10*data[sorted_indices[-1]+1]:
            outlier_index = [sorted_indices[-1]]
        else:
            outlier_index = []
    else:
        outlier_index = []
    return outlier_index

def get_header_row_by_lookup(file_content):
    matches_per_row = []
    max_match_count = 0

    rows = file_content.replace(";", ",").replace("\"", "").replace("\t", ",").replace("'","").replace(" ","_").lower().split("\n")
    for i, row in enumerate(rows):
        if i > 20:
            break

        if len(row.split()) == 0:
            continue

        column_values = row.split(",")

        total_match_count = 0
        for column_value in column_values:
            total_match_count += column_names_schemapile[column_value]

        matches_per_row.append(total_match_count)

    # testing the function
    potentials_headers = outliers_distance_to_next_order(matches_per_row)
    if len(potentials_headers):
        return max(potentials_headers) + 1, matches_per_row
    else:
        return 0, matches_per_row

for idx, f in enumerate(benchmark_files):
    in_filepath = join(IN_DIR, f)
    out_filename = f'{f}_converted.csv'
    out_filepath = join(OUT_DIR, out_filename)
    #if os.path.exists(out_filepath):
    #    continue
    print(f'Processing file ({idx + 1}/{len(benchmark_files)}) {f}')

    sut_params = load_parameters(join(PARAM_DIR, f'{f}_parameters.json'))
    for time_rep in range(N_REPETITIONS):
        start = time.time()
        try:
            with open(in_filepath, newline='', encoding=sut_params["encoding"]) as in_csvfile:
                dialect = csv.Sniffer().sniff(in_csvfile.read())
                in_csvfile.seek(0)
                content = in_csvfile.read()
                in_csvfile.seek(0)
                lookup, matches_per_row = get_header_row_by_lookup(content)
                pycsv = csv.Sniffer().has_header(content)
                reader = csv.reader(in_csvfile, dialect)
                rows = list(reader)
                hybrid = 1 if pycsv==1 else lookup
                if not hybrid:
                    rows = [["col_"+str(i) for i in range(len(rows[0]))]] + rows
                if hybrid > 1:
                    rows = rows[hybrid-1:]
            end = time.time()
            with open(out_filepath, 'w', newline='') as out_csvfile:
                csv.writer(out_csvfile).writerows(rows)

        except Exception as e:
            end = time.time()
            print("Application error on file", f)
            print("\t", e)
            with open(out_filepath, "w") as out_csvfile:
                out_csvfile.write("Application Error\n")
                out_csvfile.write(str(e))

        times_dict[f] = times_dict.get(f, []) + [(end - start)]

        try:
            del start, end, in_csvfile, dialect, reader, rows, out_csvfile
        except:
            pass

save_time_df(TIME_DIR, sut, times_dict)
