import argparse
import json
import subprocess
from collections import defaultdict
from configparser import ConfigParser
from pathlib import Path

import numpy as np

from GraphParser import mkdir_if_necessary

GRAPHS_UNWEIGHTED = []
GRAPHS_WEIGHTED = []
CODE_PATH = None
NUM_ITERS = 16

def pagerank(p, num_nodes, kind):
    print("\nGraph: {}".format(p))
    name = 'pagerank {} {}'.format(p, NUM_ITERS)
    run = run_mp if kind == 'mp' else run_upcxx
    time = run(name, num_nodes)
    return time

def bfs(p, num_nodes, kind):
    print("\nGraph: {}".format(p))
    
    name = 'bfs {} {}'.format(p, NUM_ITERS)
    run = run_mp if kind == 'mp' else run_upcxx
    time = run(name, num_nodes)
    return time

def bf(p, num_nodes, kind):
    print("\nGraph: {}".format(p))

    name = 'bellman_ford {} {}'.format(p, NUM_ITERS)
    run = run_mp if kind == 'mp' else run_upcxx
    time = run(name, num_nodes)
        
    return time

def cc(p, num_nodes, kind):
    print("\nGraph: {}".format(p))
    
    name = 'connected_components {} {}'.format(p, NUM_ITERS)
    run = run_mp if kind == 'mp' else run_upcxx
    time = run(name, num_nodes)

    return time

ALL_TESTS = {
    'bf': bf,
    'bfs': bfs,
    'cc': cc,
    'pagerank': pagerank
}

def run_mp(name, num_nodes):
    mp_path = (CODE_PATH / "openmp" / name).absolute()
    command = "OMP_NUM_THREADS={} CODE_MODE=PRODUCTION bash -c '{}'".format(num_nodes, mp_path)
    print(command)
    output = subprocess.check_output(command, shell=True)
    result = float(output.decode("utf-8")[:-1].split("\n")[-1])
    return result

def run_upcxx(name, num_nodes):
    upcxx_path = (CODE_PATH / "upcxx" / name).absolute()
    command = "CODE_MODE=PRODUCTION {}/bin/upcxx-run -n {} -shared-heap 4G {}".format(os.environ['UPCXX_INSTALL'], num_nodes, upcxx_path)
    print(command)
    output = subprocess.check_output(command, shell=True)
    result = float(output.decode("utf-8")[:-1].split("\n")[-1])
        
    return result

def get_graphs(base_p):
    unweighted = {}
    weighted = {}
    unweighted_graphs = (base_p / "unweighted").glob("*.txt")
    for unweighted_graph in unweighted_graphs:
        graph_size = int(unweighted_graph.stem)
        unweighted[graph_size] = unweighted_graph
    
    weighted_graphs = (base_p / "weighted").glob("*.txt")
    for weighted_graph in weighted_graphs:
        graph_size = int(weighted_graph.stem)
        weighted[graph_size] = weighted_graph

    return weighted, unweighted

def init():
    config = ConfigParser()
    config.read("config.ini")
    global GRAPHS_UNWEIGHTED
    global GRAPHS_WEIGHTED
    global CODE_PATH

    graph_path = Path(config.get("DEFAULT", "RealGraphPath")).parent / "powerlaw"
    GRAPHS_WEIGHTED, GRAPHS_UNWEIGHTED = get_graphs(graph_path)
    CODE_PATH = Path(config.get("DEFAULT", "CodePath"))

def main(args):
    init()

    results = {}
    tests = ALL_TESTS.keys()
    kinds = ['upcxx', 'mp'] if args.kind == 'all' else [args.kind]
    graphs = []
    for kind in kinds:
        results[kind] = {}
        for test in tests:
            if test != 'bf':
                graphs = GRAPHS_UNWEIGHTED
            else:
                graphs = GRAPHS_WEIGHTED
            results[kind][test] = []
            num_nodes_min = min(graphs.keys())
            num_nodes = 1
            while num_nodes <= 32:
                time = ALL_TESTS[test](graphs[num_nodes * num_nodes_min], num_nodes, kind)
                results[kind][test].append((num_nodes, time))
                num_nodes *= 2

    with open(args.output, "w") as f:
        json.dump(results, f)
            

if __name__ == "__main__":
    import os
    cws = os.getcwd()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if os.path.join(cws, "utils") != dir_path:
        raise Exception("run_tests_weak.py must be run from the top-level directory of the repository")

    parser = argparse.ArgumentParser(description="Run weak scaling tests on OpenMP and UPC++")
    parser.add_argument('--kind', type=str, default='all', help='Run on what platforms. "all" runs both UPC++ and OpenMP')
    parser.add_argument('--output', type=str, default="test_weak_scaling.json", help="The output file generated when the test is completed")

    args = parser.parse_args()
    main(args)
