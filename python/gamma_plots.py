"""Generate plots for some trace file while scanning through the size of Gamma.
Currently only using the DIMSUM algorithm."""
from matplotlib import pyplot as plt
import numpy as np
import subprocess

def run_with_params(file, gamma):
    HH_BINARY = '../build/hh'
    result = subprocess.run([HH_BINARY, '-f', file, '-g', str(gamma)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = str(result.stdout)
    # Our output should be right after the line with Method in the format
    # Method  Updates/ms  Space  Recall  5th  95th  Precis  5th  95th  Freq RE  5th  95th
    method_index = output.find("Method")
    if method_index == -1:
        raise ValueError("Could not parse output to find stats for some reason")
    output = str(output[method_index:])
    lines = output.split("\\n")
    lines = [line.split("\\t") for line in lines]
    
    # ALS update/ms will be the second index of second
    updates_als = lines[1][1]
    return float(updates_als)


if __name__ == '__main__':
    updates_ms = []
    DATASET = "../trace/sanjose.dmp"
    GAMMAS = [2**x for x in range(-4, 5)]
    NUM_RUNS = 10
    for gamma in GAMMAS:
        print('Running gamma = %f' % gamma)
        updates = [run_with_params(DATASET, gamma) for _ in range(NUM_RUNS)]
        updates_ms.append(np.array(updates).mean())

    fig = plt.figure()
    plt.plot(GAMMAS, updates_ms, marker='o')
    plt.xscale('log')
    plt.xticks(GAMMAS, GAMMAS)
    plt.title('Deamortized Algorithm (DIMSUM)')
    plt.ylabel('Updates/ms')
    plt.xlabel(r'$\gamma$')
    plt.show()
