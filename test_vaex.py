import time as t
import vaex
import sys

def import_file(file_name):
    t0 = t.time()
    df = vaex.from_csv(file_name, convert=True, nrows=5_000_000)
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
#    df_aux = df.concat(df)
#    print(df_aux.shape)
    t1 = t.time()
    print("Concat columns (seconds): {0}".format(t1 - t0))
    return df

def concat_rows(df):
    t0 = t.time()
    df_aux = vaex.concat([df, df])
    t1 = t.time()
    print("Concat rows (seconds): {0}".format(t1 - t0))
    return df


def join_dfs(df_a, df_b):
    t0 = t.time()
    df_suffix = df_a.join(df_b, left_on='0', right_on='4', how='right', suffix='_a')
    t1 = t.time()
    print("Join on different columns (seconds): {0}".format(t1 - t0))


def run_tests(file_name):
    df = import_file(file_name)
    get_head(df)
    get_tail(df)
    df = concat_columns(df)
    df = concat_rows(df)
    join_dfs(df, df)


if __name__ == '__main__':
    run_tests("data_1GB.csv")
