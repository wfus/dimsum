"""Generate plots for some trace file while scanning through the size of Gamma.
Currently only using the DIMSUM algorithm."""
from matplotlib import pyplot as plt
import numpy as np
import subprocess


def run_with_params(file, gamma, mapping, runs=1):
    """Takes in a file with a list of gamma values to test. Then, returns a
    dictionary mapping the 
        @param gamma: Gamma value to try out for the experiment. 
        @param mapping: mapping from algorithm type to line number. Line number
            starts with 1. Example is {'als':1, 'dimsumpp':2, 'dimsum':3}
        @param runs: number of runs to average over.

    Returns:
        Dictionary keyed by algorithm. Value will be a tuple of two elements,
        one the size of the datastructure for this gamma, and the updates/ms
    """
    # Initialize our results dictionaries
    update_times = {}
    sizes = {}
    for k in mapping.keys():
        update_times[k] = []
        sizes[k] = []

    for _ in range(runs):
        HH_BINARY = '../build/hh'
        result = subprocess.run([HH_BINARY, '-f', file, '-gamma', str(gamma)],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = str(result.stdout)
        # Our output should be right after the line with Method in the format
        # Method  Updates/ms  Space  Recall  5th  95th  Precis  5th  95th  Freq RE  5th  95th
        method_index = output.find("Method")
        if method_index == -1:
            print(result.stderr)
            print("Could not parse output to find stats for some reason")
            # raise ValueError("Could not parse output to find stats for some reason")
            continue
        output = str(output[method_index:])
        lines = output.split("\\n")
        lines = [line.split("\\t") for line in lines]
        
        # Iterate through our algorithms we want to record and scrape out the
        # updates/ms and size columns, which should be 1 and 2 respectively.
        for algo, line_num in mapping.items():
            update_times[algo].append(float(lines[line_num][1]))
            sizes[algo].append(float(lines[line_num][2]))
    
    # Return a mapping of algorithm to the agerage of these two.
    return {k: (np.mean(update_times[k]), np.mean(sizes[k])) for k in mapping.keys()}


if __name__ == '__main__':
    DATASET = "../trace/sanjose.dmp"
    NUM_RUNS = 10
    GAMMAS = [0.8, 1, 1.2, 2, 3, 4, 5, 6, 7, 8]
    LINE_MAPPING = {'IMSUM': 1,
                    'DIMSUM++': 2,
                    'DIMSUM': 3}

    # Initialize our dictionary of results
    update_speed_d = {}
    size_d = {}
    for k in LINE_MAPPING.keys():
        update_speed_d[k] = []
        size_d[k] = []

    # Run actual experiment!
    for gamma in GAMMAS:
        print('Running gamma = %f' % gamma)
        ret = run_with_params(DATASET, gamma, LINE_MAPPING)
        for k, v in ret.items():
            update_speed_d[k].append(v[0])
            size_d[k].append(v[1])
    print(update_speed_d)

    fig = plt.figure()
    for key in update_speed_d.keys():
        plt.plot(size_d[key], update_speed_d[key], marker='o')
    plt.title('Sizes of Data Structure vs Runtime')
    plt.ylabel('Updates/ms')
    plt.xlabel(r'Space Used (Bytes)')
    plt.legend(update_speed_d.keys())
    plt.show()

    """
    # Plotting with logscale
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
    """
