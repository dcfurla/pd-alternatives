import time as t
import pandas as pds
import sys

def import_file(file_name):
    t0 = t.time()
    df = pds.read_csv(file_name)
    print(df.columns)
    t1 = t.time()
    print("Import (seconds): {0}. Rows: {1}".format(t1 - t0, len(df)))
    return df

def get_head(df):
    t0 = t.time()
    head = df.head(5)
    t1 = t.time()
    print("Head (seconds): {0}".format(t1 - t0))

def get_tail(df):
    t0 = t.time()
    head = df.tail(5)
    t1 = t.time()
    print("Tail (seconds): {0}".format(t1 - t0))

def concat_columns(df):
    t0 = t.time()
    df_aux = pds.concat([df, df], axis=1)
    t1 = t.time()
    print("Concat columns (seconds): {0}".format(t1 - t0))
    return df

def concat_rows(df):
    t0 = t.time()
    df_aux = pds.concat([df, df], axis=0)
    t1 = t.time()
    print("Concat rows (seconds): {0}".format(t1 - t0))
    return df


def join_dfs(df_a, df_b):
    t0 = t.time()
    df_suffix = pds.merge(df_a, df_b, left_on="1210", right_on="4", how='outer', suffixes=('_left', '_right'))
    t1 = t.time()
    print("Join on different columns (seconds): {0}".format(t1 - t0))

def groupby(df):
    t0 = t.time()
    gp = df.groupby(by=['cat']).sum()
    t1 = t.time()
    print("Groupby time: {0}".format(t1 - t0))

def run_tests(file_name):
    df = import_file(file_name)
    df = concat_columns(df)
    df = concat_rows(df)
    get_head(df)
    get_tail(df)
    join_dfs(df, df)
    groupby(df)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        run_tests(sys.argv[1])
    else:
        print('Please provide the name of the input file')
