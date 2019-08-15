import argparse
import numpy as np
import subprocess
import json

from GraphParser import mkdir_if_necessary
from configparser import ConfigParser
from collections import defaultdict
from pathlib import Path


GRAPHS_UNWEIGHTED = []
GRAPHS_WEIGHTED = []
CODE_PATH = None

def pagerank(p, num_nodes, kind):
    print("\nGraph: {}".format(p))
    name = 'pagerank {} {}'.format(p, NUM_ITERS)
    run = run_mp if kind == 'mp' else run_upcxx
    time = run(name, num_nodes)[0]
    return time

def bfs(p, num_nodes, kind):
    print("\nGraph: {}".format(p))
    
    name = 'bfs {} {}'.format(p, NUM_ITERS)
    run = run_mp if kind == 'mp' else run_upcxx
    time = run(name, num_nodes)[0]
    return time

def bf(p, num_nodes, kind):
    print("\nGraph: {}".format(p))

    name = 'bellman_ford {} {}'.format(p, NUM_ITERS)
    run = run_mp if kind == 'mp' else run_upcxx
    time = run(name, num_nodes)[0]
        
    return time

def cc(p, num_nodes, kind):
    print("\nGraph: {}".format(p))
    
    name = 'connected_components {} {}'.format(p, NUM_ITERS)
    run = run_mp if kind == 'mp' else run_upcxx
    time = run(name, num_nodes)[0]

    return time

ALL_TESTS = {
    'bf': bf,
    'bfs': bfs,
    'cc': cc,
    'pagerank': pagerank
}


def run_mp(name, num_nodes):
    mp_path = (CODE_PATH / "openmp" / name).absolute()
    command = "OMP_NUM_THREADS={} bash -c '{}'".format(num_nodes, mp_path)
    print(command)
    output = subprocess.check_output(command, shell=True)
    result = [float(f) for f in output.decode("utf-8")[:-1].split("\n")]
    return result

def run_upcxx(name, num_nodes):
    upcxx_path = (CODE_PATH / "upcxx" / name).absolute()
    command = "{}/bin/upcxx-run -n {} {}".format(os.environ['UPCXX_INSTALL'], num_nodes, upcxx_path)
    print(command)
    output = subprocess.check_output(command, shell=True)
    result = [float(f) for f in output.decode("utf-8")[:-1].split("\n")]
        
    return result

def get_real_graphs(base_p):
    unweighted = []
    weighted = []
    unweighted_graphs = (base_p / "unweighted").glob("*.txt")
    for unweighted_graph in unweighted_graphs:
        with open(unweighted_graph) as f:
            node_size = int(f.readline()[:-1])
            edge_size = int(f.readline()[:-1])
            d = {'node_size':node_size, 'edge_size':edge_size, 'name':unweighted_graph.stem }
            unweighted.append((unweighted_graph, d))
    
    weighted_graphs = (base_p / "weighted").glob("*.txt")
    for weighted_graph in weighted_graphs:
        with open(weighted_graph) as f:
            node_size = int(f.readline()[:-1])
            edge_size = int(f.readline()[:-1])
            neg_rate = 0.09 if weighted_graph.stem == "soc-sign-bitcoinalpha" else 0.11
            d = {'node_size':node_size, 'edge_size':edge_size, 'name':weighted_graph.stem, 'neg_rate': neg_rate}
            weighted.append((weighted_graph, d))

    return weighted, unweighted

def init(args):
    config = ConfigParser()
    config.read("config.ini")
    global GRAPHS_UNWEIGHTED
    global GRAPHS_WEIGHTED
    global CODE_PATH
    global NUM_ITERS

    weighted, unweighted = get_real_graphs(Path(config.get("DEFAULT", "RealGraphPath")))
    GRAPHS_WEIGHTED += weighted
    GRAPHS_UNWEIGHTED += unweighted
    CODE_PATH = Path(config.get("DEFAULT", "CodePath"))
    NUM_ITERS = args.num_iters
    
def main(args):
    num_nodes_min = args.num_nodes_min
    num_nodes_max = args.num_nodes_max

    init(args)

    results = {}
    tests = ALL_TESTS.keys() if args.test == 'all' else [args.test]
    kinds = ['upcxx', 'mp'] if args.test == 'all' else [args.test]
    for test in tests:
        results[test] = {}
        if test != 'bf':
            graphs = [GRAPHS_UNWEIGHTED
        else:
            graphs = GRAPHS_WEIGHTED
        if args.graph != 'all':
            graphs = [(p,d) for p,d in graphs if d['name'] == args.graph]
        for graph in graphs:
            p, info = graph
            results[test][info['name']] = {'info':info, 'data':[]}
            for kind in kinds:
                num_nodes = num_nodes_min
                while num_nodes <= num_nodes_max:
                    try:
                        time = ALL_TESTS[test](p, num_nodes, kind)
                        print(time)
                        results[test][info['name']]['data'].append((num_nodes, time))
                    except Exception as e:
                        print(e)
                        print("Error running {} {} {}".format(test, graph, kind))
                    num_nodes *= 2
    
    with open(args.output, "w") as f:
        json.dump(results, f)
            

if __name__ == "__main__":
    import os
    cws = os.getcwd()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if os.path.join(cws, "utils") != dir_path:
        raise Exception("run_tests.py must be run from the top-level directory of the repository")
    
    parser = argparse.ArgumentParser(description="Run tests on OpenMP and UPC++")
    parser.add_argument('--test', type=str, default='all', help='The test(s) to run. "all" runs all tests')
    parser.add_argument('--kind', type=str, default='all', help='Run on what platforms. "all" runs both UPC++ and OpenMP')
    parser.add_argument('--graph', type=str, default='all', help='What graphs to run tests on. "all" runs all available graphs')
    parser.add_argument('--num_nodes_min', type=int, default=1, help="The minimum number of nodes on which to run tests. Running all tests would try node counts of [num_nodes_min, num_nodes_max]")
    parser.add_argument('--num_nodes_max', type=int, default=32, help="The maximum number of nodes on which to run tests. Running all tests would try node counts of [num_nodes_min, num_nodes_max]")
    parser.add_argument('--output', type=str, default="test_results_real.json", help="The output file generated when the test is completed")
    parser.add_argument('--num_iters', type=int, default=16, help="The number of iterations for each test")
    

    args = parser.parse_args()
    
    
    main(args)