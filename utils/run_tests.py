import os
import sys
import subprocess
import json
import argparse

from configparser import ConfigParser
from pathlib import Path


def parse_list(s, t=int):
    return [t(v) for v in s.split(" ")]

def get_random_graphs(base_p):
    config = ConfigParser()
    config.read("config.ini")
    
    node_sizes = parse_list(config.get("DEFAULT", "NodeSizes"))
    edge_proportions = parse_list(config.get("DEFAULT", "EdgeProportions"))
    neg_rates = parse_list(config.get("DEFAULT", "NegRates"), float)
    copies = int(config.get("DEFAULT", "Copies"))

    unweighted = []
    weighted = []
    for node_size in node_sizes:
        for edge_proportion in edge_proportions:
            unweighted_base = base_p / "unweighted"
            for i in range(copies):
                p = unweighted_base / "{}_{}_{}.txt".format(node_size, edge_proportion, i)
                d = {"node_size":node_size, "edge_size": node_size * edge_proportion, "edge_proportion":edge_proportion, "copy":i}
                unweighted.append((p, d))

    for node_size in node_sizes:
        for edge_proportion in edge_proportions:
            weighted_base = base_p / "weighted"
            for neg_rate in neg_rates:
                for i in range(copies):
                    p = weighted_base / "{}_{}_{}_{}.txt".format(node_size, edge_proportion, neg_rate, i)
                    d = {"node_size":node_size, "edge_size": node_size * edge_proportion, "edge_proportion":edge_proportion, "neg_rate":neg_rate, "copy": i}
                    weighted.append((p, d))

    return weighted, unweighted

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

def get_array_sizes():
    array_sizes = [10**i for i in range(2,7)]
    return array_sizes


def run_mp(name, num_nodes):
    mp_path = (CODE_PATH / "openmp" / name).absolute()
    command = "OMP_NUM_THREADS={} bash -c '{}'".format(num_nodes, mp_path)
    if name == "hello":
        os.system(command)
        return
    output = subprocess.check_output(command, shell=True)
    result = [float(f) for f in output.decode("utf-8")[:-1].split("\n")]
    return result

def run_upcxx(name, num_nodes):
    upcxx_path = (CODE_PATH / "upcxx" / name).absolute()
    command = "{}/bin/upcxx-run -n {} {}".format(os.environ['UPCXX_INSTALL'], num_nodes, upcxx_path)
    if name == "hello":
        os.system(command)
        return
    output = subprocess.check_output(command, shell=True)
    result = [float(f) for f in output.decode("utf-8")[:-1].split("\n")]
        
    return result

def hello_mp(num_nodes):
    run_mp('hello', num_nodes)

def hello_upcxx(num_nodes):
    run_upcxx('hello', num_nodes)

def random_access_mp(num_nodes):
    result = []
    for array_size in ARRAY_SIZES:
        print("Array size:", array_size)
        name = 'random_access {}'.format(array_size)
        try:
            read_time, write_time = run_mp(name, num_nodes)
            result.append({'error':False,'read':read_time, 'write':write_time, 'array_size': array_size})
        except:
            print("Error when running {} with {} cores".format(name, num_nodes))
            result.append({'error':True})
    return result

def random_access_upcxx(num_nodes):
    result = []
    for array_size in ARRAY_SIZES:
        print("Array size:", array_size)
        name = 'random_access_future {}'.format(array_size)
        try:
            read_time, write_time = run_upcxx(name, num_nodes)
            result.append({'error':False, 'read':read_time, 'write':write_time, 'array_size': array_size})
        except:
            print("Error when running {} with {} cores".format(name, num_nodes))
            result.append({'error':True})
    return result

def pagerank_mp(num_nodes):
    result = []
    for p, info in GRAPHS_UNWEIGHTED:
        print("\nGraph: {} nodes; {} edges\n".format(info['node_size'],info['edge_size']))
        name = 'pagerank {}'.format(p)
        try:
            time = run_mp(name, num_nodes)
            result.append({'error':False, 'time':time[0], **info})
        except:
            print("Error when running {} with {} cores".format(name, num_nodes))
            result.append({'error':True})
    return result

def pagerank_upcxx(num_nodes):
    result = []
    for p, info in GRAPHS_UNWEIGHTED:
        print("\nGraph: {} nodes; {} edges\n".format(info['node_size'],info['edge_size']))
        name = 'pagerank {}'.format(p)
        try:
            time = run_upcxx(name, num_nodes)
            result.append({'error':False, 'time':time[0], **info})
        except:
            print("Error when running {} with {} cores".format(name, num_nodes))
            result.append({'error':True})
    return result

def bfs_mp(num_nodes):
    result = []
    for p, info in GRAPHS_UNWEIGHTED:
        print("\nGraph: {} nodes; {} edges\n".format(info['node_size'],info['edge_size']))
        name = 'bfs {} 0'.format(p)
        try:
            time = run_mp(name, num_nodes)
            result.append({'error':False, 'time':time[0], **info})
        except:
            print("Error when running {} with {} cores".format(name, num_nodes))
            result.append({'error':True})
    return result

def bfs_upcxx(num_nodes):
    result = []
    for p, info in GRAPHS_UNWEIGHTED:
        print("\nGraph: {} nodes; {} edges\n".format(info['node_size'],info['edge_size']))
        name = 'bfs {} 0'.format(p)
        try:
            time = run_upcxx(name, num_nodes)
            result.append({'error':False, 'time':time[0], **info})
        except:
            print("Error when running {} with {} cores".format(name, num_nodes))
            result.append({'error':True})
    return result

def bf_mp(num_nodes):
    result = []
    for p, info in GRAPHS_WEIGHTED:
        if 'name' not in info and info['node_size'] > 1000 and info['neg_rate'] > 0:
            continue
        print("\nGraph: {} nodes; {} edges\n".format(info['node_size'],info['edge_size']))
        name = 'bellman_ford {} 0'.format(p)
        try:
            time = run_mp(name, num_nodes)
            result.append({'error':False, 'time':time[0], **info})
        except:
            print("Error when running {} with {} cores".format(name, num_nodes))
            result.append({'error':True})
    return result

def bf_upcxx(num_nodes):
    result = []
    for p, info in GRAPHS_WEIGHTED:
        if 'name' not in info and info['node_size'] > 1000 and info['neg_rate'] > 0:
            continue
        print("\nGraph: {} nodes; {} edges\n".format(info['node_size'],info['edge_size']))
        name = 'bellman_ford {} 0'.format(p)
        try:
            time = run_upcxx(name, num_nodes)
            print(time)
            result.append({'error':False, 'time':time[0], **info})
        except:
            print("Error when running {} with {} cores".format(name, num_nodes))
            result.append({'error':True})
    return result
    
def cc_mp(num_nodes):
    result = []
    for p, info in GRAPHS_UNWEIGHTED:
        print("\nGraph: {} nodes; {} edges\n".format(info['node_size'],info['edge_size']))
        name = 'connected_components {}'.format(p)
        try:
            time = run_mp(name, num_nodes)
            result.append({'error':False, 'time':time[0], **info})
        except:
            print("Error when running {} with {} cores".format(name, num_nodes))
            result.append({'error':True})
    return result

    
def cc_upcxx(num_nodes):
    result = []
    for p, info in GRAPHS_UNWEIGHTED:
        print("\nGraph: {} nodes; {} edges\n".format(info['node_size'],info['edge_size']))
        name = 'connected_components {}'.format(p)
        try:
            time = run_upcxx(name, num_nodes)
            result.append({'error':False, 'time':time[0], **info})
        except:
            print("Error when running {} with {} cores".format(name, num_nodes))
            result.append({'error':True})
    return result

from collections import defaultdict
all_results = defaultdict(dict)

def run_task(name, mp, upcxx, num_nodes):
    all_results[num_nodes][name] = {}
    print("-------------------")
    print("Task {}".format(name))
    print("Nodes:", num_nodes)
    print("-------------------")
    print("\n1. OpenMP:\n")
    result_mp = mp(num_nodes)
    all_results[num_nodes][name]["mp"] = result_mp
    print("\n2. UPC++:\n")
    result_upcxx = upcxx(num_nodes)
    all_results[num_nodes][name]["upcxx"] = result_upcxx
    print("")

ALL_TESTS = {
    'hello': {'mp': hello_mp, 'upcxx': hello_upcxx},
    'random_access': {'mp': random_access_mp, 'upcxx': random_access_upcxx},
    'pagerank': {'mp': pagerank_mp, 'upcxx': pagerank_upcxx},
    'bfs': {'mp': bfs_mp, 'upcxx': bfs_upcxx},
    'bf': {'mp': bf_mp, 'upcxx': bf_upcxx},
    'cc': {'mp': cc_mp, 'upcxx': cc_upcxx}
}

def main(args):
    test = args.test
    num_nodes_min = args.num_nodes_min
    num_nodes_max = args.num_nodes_max
    if num_nodes_min < 2:
        raise Exception("num_nodes_min must be at least 2")

    init(args) # read graphs

    if test == "all":
        tests = ALL_TESTS.keys()
    else:
        tests = [test]

    for num_node in range(num_nodes_min, num_nodes_max+1, 4):
        for name in tests:
            run_task(name, ALL_TESTS[name]['mp'], ALL_TESTS[name]['upcxx'], num_node)

    # print(all_results)
        
    if test == "all":
        with open(args.output, "w") as f:
            json.dump(all_results, f)

GRAPHS_UNWEIGHTED = []
GRAPHS_WEIGHTED = []
CODE_PATH = None

def init(args):
    config = ConfigParser()
    config.read("config.ini")
    global GRAPHS_UNWEIGHTED
    global GRAPHS_WEIGHTED
    global ARRAY_SIZES
    global CODE_PATH

    if args.dataset in ['random', 'all']:
        weighted, unweighted = get_random_graphs(Path(config.get("DEFAULT", "RandomGraphPath")))
        GRAPHS_WEIGHTED += weighted
        GRAPHS_UNWEIGHTED += unweighted
    if args.dataset in ['real', 'all']:
        weighted, unweighted = get_real_graphs(Path(config.get("DEFAULT", "RealGraphPath")))
        GRAPHS_WEIGHTED += weighted
        GRAPHS_UNWEIGHTED += unweighted
    ARRAY_SIZES = get_array_sizes()
    CODE_PATH = Path(config.get("DEFAULT", "CodePath"))

if __name__ == "__main__":
    import os
    cws = os.getcwd()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if os.path.join(cws, "utils") != dir_path:
        raise Exception("run_tests.py must be run from the top-level directory of the repository")
    
    parser = argparse.ArgumentParser(description="Run tests on OpenMP and UPC++")
    parser.add_argument('test', type=str, help="The name of the test to be run. Input all for a proper test run")
    parser.add_argument('--num_nodes_min', type=int, default=1, help="The minimum number of nodes on which to run tests. Running all tests would try node counts of [num_nodes_min, num_nodes_max]")
    parser.add_argument('--num_nodes_max', type=int, default=32, help="The maximum number of nodes on which to run tests. Running all tests would try node counts of [num_nodes_min, num_nodes_max]")
    parser.add_argument('--output', type=str, default="test_results.json", help="The output file generated when the test is completed")
    parser.add_argument('--dataset', type=str, default="random", choices=['random', 'real', 'all'], help="The graphs to use for testing")

    args = parser.parse_args()
    main(args)