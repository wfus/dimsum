"""Generate plots for some trace file while scanning through the size of Gamma.
Currently only using the DIMSUM algorithm."""
from matplotlib import pyplot as plt
import numpy as np
import subprocess

def run_with_params(file, gamma):
    HH_BINARY = '../build/hh'
    result = subprocess.run([HH_BINARY, '-f', file, '-gamma', str(gamma)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = str(result.stdout)
    # Our output should be right after the line with Method in the format
    # Method  Updates/ms  Space  Recall  5th  95th  Precis  5th  95th  Freq RE  5th  95th
    method_index = output.find("Method")
    if method_index == -1:
        print(output)
        return
        # raise ValueError("Could not parse output to find stats for some reason")
    output = str(output[method_index:])
    lines = output.split("\\n")
    lines = [line.split("\\t") for line in lines]
    
    # ALS update/ms will be the second index of second
    # DIMpp update/ms will be second index of third
    updates_als = lines[1][1]
    updates_dimsumpp = lines[2][1]
    print(lines)
    return (float(updates_als), float(lines[1][2])), (float(updates_dimsumpp), float(lines[2][2]))


if __name__ == '__main__':
    als_ms = []
    dimsumpp_ms = []
    als_sizes = []
    dimsumpp_sizes = []
    DATASET = "../trace/sanjose.dmp"
    GAMMAS = [2**-4, 0.12, 2**-3, 2**-2, 1, 2, 4, 8]
    NUM_RUNS = 10
    for gamma in GAMMAS:
        print('Running gamma = %f' % gamma)
        times = [run_with_params(DATASET, gamma) for _ in range(NUM_RUNS)]
        als_times, dimsumpp_times = zip(*times)
        als_ms.append(np.array([a[0] for a in als_times]).mean())
        dimsumpp_ms.append(np.array([a[0] for a in dimsumpp_times]).mean())
        als_sizes.append(np.array([a[1] for a in als_times]).mean())
        dimsumpp_sizes.append(np.array([a[1] for a in dimsumpp_times]).mean())

    fig = plt.figure()
    plt.plot(GAMMAS, als_ms, marker='o')
    plt.plot(GAMMAS, dimsumpp_ms, marker='o')
    plt.xscale('log')
    plt.xticks(GAMMAS, GAMMAS)
    plt.title('Deamortized Algorithm (DIMSUM)')
    plt.ylabel('Updates/ms')
    plt.xlabel(r'$\gamma$')
    plt.legend(['IM-SUM', 'DIM-SUM++'])
    plt.show()


    fig = plt.figure()
    plt.plot(als_sizes, als_ms, marker='o')
    plt.plot(dimsumpp_sizes, dimsumpp_ms, marker='o')
    plt.title('Deamortized Algorithm (DIMSUM)')
    plt.ylabel('Updates/ms')
    plt.xlabel(r'Space Used (Bytes)')
    plt.legend(['IM-SUM', 'DIM-SUM++'])
    plt.show()
