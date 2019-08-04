import os
import argparse
import json
import numpy as np
from GraphParser import mkdir_if_necessary
from configparser import ConfigParser
from collections import defaultdict
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt



def parse_list(s, t=int):
    return [t(v) for v in s.split(" ")]

TESTS = ['random_access', 'pagerank', 'bfs', 'bf', 'cc']
TESTS_UNWEIGHTED = ['pagerank' ,'bfs', 'cc']
TESTS_WEIGHTED = ['bf']
TESTS_ARRAY = ['random_access']

KIND_MAP = {
    "upcxx": "UPC++",
    "mp": "OpenMP"
}

def get_key(**key_pairs):
    return tuple(key_pairs.items())

def graph_array(xs, ys_read, ys_write, test, array_size):
    ys = {'read':ys_read, 'write':ys_write}
    fig, axes = plt.subplots(nrows=2, figsize=(12, 12), dpi=300)
    for i, rw in enumerate(['read', 'write']):
        title = "{} {}: {}".format(test, rw, array_size)
        ax = axes[i]
        ax.set_title(title)
        for kind in ['upcxx', 'mp']:
            ax.plot(xs, ys[rw][kind], label=KIND_MAP[kind], marker='.')
            #for i_x, i_y in zip(xs, ys[rw][kind]):
            #    ax.text(i_x, i_y, '{}s'.format(i_y))
            
        ax.legend()
        ax.set_xlabel("Nodes")
        ax.set_ylabel("Time (ms)")
    outpath = output_dir / test / "{}.png".format(array_size)
    plt.savefig(outpath, dpi=300)
    plt.close(fig)

def graph_unweighted(xs, ys, test, **params):
    fig, ax = plt.subplots(figsize=(12, 12), dpi=300)
    title = "{}: {} Nodes | {} Edges".format(test, params['node_size'], params['edge_size'])
    ax.set_title(title)
    for kind in ['upcxx', 'mp']:
        ax.plot(xs, ys[kind], label=kind, marker='.')
        #for i_x, i_y in zip(xs, ys[kind]):
        #    ax.text(i_x, i_y, '{}s'.format(i_y))
        
    ax.legend()
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Time (ms)")
    
    outpath = output_dir / test / "{}_{}.png".format(params['node_size'], params['edge_size'])
    plt.savefig(outpath, dpi=300)
    plt.close(fig)

def graph_weighted(xs, ys, test, **params):
    fig, ax = plt.subplots(figsize=(12, 12), dpi=300)
    title = "{}: {} Nodes | {} Edges | {}% Negative".format(test, params['node_size'], params['edge_size'], params['neg_rate']*100)
    ax.set_title(title)
    for kind in ['upcxx', 'mp']:
        ax.plot(xs, ys[kind], label=kind, marker='.')
        #for i_x, i_y in zip(xs, ys[kind]):
        #    ax.text(i_x, i_y, '{}s'.format(i_y))
        
    ax.legend()
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Time (ms)")
    
    outpath = output_dir / test / "{}_{}_{}.png".format(params['node_size'], params['edge_size'], params['neg_rate'])
    plt.savefig(outpath, dpi=300)
    plt.close(fig)

def is_use_line(line, **params):
    # this is the normal version
    for k, v in params.items():
        if k not in line.keys():
            return False
        if line[k] != v:
            return False
    return True

def get_data_tests_array(data, test, **params):
    xs = list(range(2, num_cores, 4))
    ys = {
        'upcxx': [],
        'mp': []
    }
    rw = params.pop('rw')
    for x in xs:
        for kind in ['upcxx', 'mp']:
            for line in data[str(x)][test][kind]:
                if not is_use_line(line, **params):
                    continue
                ys[kind].append(line[rw])

    return xs, ys

def get_data_tests_graphs(data, test, **params):
    xs = list(range(2, num_cores, 4))
    ys = {
        'upcxx': [],
        'mp': []
    }
    for x in xs:
        for kind in ['upcxx', 'mp']:
            for line in data[str(x)][test][kind]:
                if not is_use_line(line, **params):
                    continue
                ys[kind].append(line['time'])
    return xs, ys

def get_data(data, test, **params):
    if test in TESTS_ARRAY:
        xs, ys = get_data_tests_array(data, test, **params)
    elif test in TESTS_UNWEIGHTED:
        xs, ys = get_data_tests_graphs(data, test, **params)
    elif test in TESTS_WEIGHTED:
        xs, ys = get_data_tests_graphs(data, test, **params)
    
    for kind in ['upcxx', 'mp']:
        ys[kind] = [v*1000 for v in ys[kind]] # into ms
    return xs, ys

def reformat_data(data):
    xs = list(range(2, num_cores, 4))
    for test in TESTS_UNWEIGHTED + TESTS_WEIGHTED:
        for x in xs:
            for kind in ['upcxx', 'mp']:
                new_lines = []
                for i, line in enumerate(data[str(x)][test][kind]):
                    if line['copy'] == 0:
                        new_lines.append(line)
                    else:
                        new_lines[-1]['time']+=line['time']
                    if (i+1)%copies == 0: # if this is the last copy of the group
                        new_lines[-1]['time'] /= copies

                data[str(x)][test][kind] = new_lines
    return data

def main(args):
    mkdir_if_necessary(output_dir)
    with open(input_file_name) as f:
        data = json.load(f)
    data = reformat_data(data)
    
    for test in TESTS:
        mkdir_if_necessary(output_dir / test)
    
    for test in TESTS_ARRAY:
        print(test)
        for array_size in array_sizes:
            xs, ys_read = get_data(data, test, array_size=array_size, rw='read')
            xs, ys_write = get_data(data, test, array_size=array_size, rw='write')
            graph_array(xs, ys_read, ys_write, test, array_size)
    
    for test in TESTS_UNWEIGHTED:
        print(test)
        for node_size in node_sizes:
            for edge_proportion in edge_proportions:
                xs, ys = get_data(data, test, node_size=node_size, edge_proportion=edge_proportion)
                graph_unweighted(xs, ys, test, node_size=node_size, edge_size=int(node_size*edge_proportion))

    for test in TESTS_WEIGHTED:
        print(test)
        for node_size in node_sizes:
            for edge_proportion in edge_proportions:
                for neg_rate in neg_rates:
                    if node_size > 1000 and neg_rate > 0:
                        continue
                    xs, ys = get_data(data, test, node_size=node_size, edge_proportion=edge_proportion, neg_rate=neg_rate)
                    graph_weighted(xs, ys, test, node_size=node_size, edge_size=int(node_size*edge_proportion), neg_rate=neg_rate)
                

if __name__ == "__main__":
    cws = os.getcwd()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if os.path.join(cws, "utils") != dir_path:
        raise Exception("run_tests.py must be run from the top-level directory of the repository")
    
    parser = argparse.ArgumentParser(description="Visualize the data")
    parser.add_argument('--input', type=str, default="test_result.json", help="The output file generated when the test is completed")
    parser.add_argument("--output_dir", type=str, default="output/", help="The directory to output the data to")

    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    input_file_name = args.input

    config = ConfigParser()
    config.read("config.ini")
    
    node_sizes = parse_list(config.get("DEFAULT", "NodeSizes"))
    edge_proportions = parse_list(config.get("DEFAULT", "EdgeProportions"))
    neg_rates = parse_list(config.get("DEFAULT", "NegRates"), float)
    copies = int(config.get("DEFAULT", "Copies"))
    array_sizes = parse_list(config.get("DEFAULT", "ArraySizes"))
    num_cores = 72
    main(args)