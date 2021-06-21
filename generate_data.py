
import numpy as np

def generate_data(mb):
    ncols = 2000
    nrows = mb*50
    mat = np.random.rand(nrows, ncols)
    #[...,None] transforms the randint array in a column array
    new_col = np.random.randint(low=0, high=20, size=len(mat))[...,None]
    mat = np.append(mat, new_col, 1)
    print('Saving data...')
    col_names = [i for i in range(0, ncols)]
    col_names[-1] = 'cat'
    col_names = ",".join([str(e) for e in col_names])
    np.savetxt('data_{0}MB.csv'.format(str(mb)), mat, delimiter=',', header=col_names, fmt='%10.5f')


if __name__ == '__main__':
    print('Generating data...')
    generate_data(2000)
    print('Done')

