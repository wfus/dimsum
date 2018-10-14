from matplotlib import pyplot as plt
import numpy as np


if __name__ == '__main__':
    with open('../trace/ucla-tcp-processed', 'r') as f:
        arr = f.readlines()
    arr = [a.split(' ') for a in arr]
    ids = [int(a[0]) for a in arr]
    sizes = [int(a[1]) for a in arr]
    plt.hist(sizes, bins=100)
    plt.show()
        