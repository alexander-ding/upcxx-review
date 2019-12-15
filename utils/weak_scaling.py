""" Runs weak scaling tests on power law graphs
"""
import argparse
import json
import os
import subprocess
from pathlib import Path

from utils import (CODE_PATH, GRAPH_PATH_PRODUCTION, GRAPH_PATH_TEST,
                   UTILS_PATH, add_weights_graph, check_cwd,
                   mkdir_if_necessary)


def get_graphs(args):
    graph_paths = {}
    num_nodes = args.num_nodes_min
    GRAPH_PATH = GRAPH_PATH_PRODUCTION if args.is_production else GRAPH_PATH_TEST
    while num_nodes <= args.num_nodes_max:
        graph_path = GRAPH_PATH / "powerlaw" / "unweighted" / f"{num_nodes*args.n}.txt"
        graph_path_weighted = GRAPH_PATH / "powerlaw" / "weighted" / f"{num_nodes*args.n}.txt"
        assert graph_path.exists()
        graph_paths[num_nodes] = [graph_path, graph_path_weighted, f"{num_nodes*args.n}.txt"]
        num_nodes *= 2
   
    return graph_paths

def run(name, graph, kind, num_nodes, num_iters):
    if name == 'bellman_ford':
        graph = graph[1]
    else:
        graph = graph[0]
    run_command = f"{name} {graph} {num_iters}"

    if kind == "openmp":
        command = "OMP_NUM_THREADS={} CODE_MODE=PRODUCTION {}".format(num_nodes, (CODE_PATH / "openmp" / run_command).absolute())
    else:
        command = "CODE_MODE=PRODUCTION {}/bin/upcxx-run -n {} -N {} -shared-heap 4G {}".format(os.environ['UPCXX_INSTALL'], num_nodes, num_nodes, (CODE_PATH / "upcxx" / run_command).absolute())
    print(command)
    output = subprocess.check_output(command, shell=True)
    result = float(output.decode("utf-8")[:-1].split("\n")[-1])
    print(result)
    return result

algorithms = ['bfs', 'bellman_ford', 'pagerank', 'connected_components']

def merge_jsons(old_json, new_json, args, graph_paths):
    if args.kind not in old_json.keys():
        old_json[args.kind] = {}
    for algorithm in algorithms:
        if algorithm not in old_json[args.kind].keys():
            old_json[args.kind][algorithm] = {}
        num_nodes = args.num_nodes_min
        while num_nodes <= args.num_nodes_max:
            old_json[args.kind][algorithm][num_nodes] = new_json[args.kind][algorithm][num_nodes]
            num_nodes *= 2
    return old_json

def main(args):
    graph_paths = get_graphs(args)

    output_json = {}
    output_json[args.kind] = {}
    for algorithm in algorithms:
        output_json[args.kind][algorithm] = {}
        num_nodes = args.num_nodes_min
        while num_nodes <= args.num_nodes_max:
            graph = graph_paths[num_nodes]
            runtime = run(algorithm, graph, args.kind, num_nodes, args.num_iters)
            output_json[args.kind][algorithm][num_nodes] = runtime
            num_nodes *= 2

    if Path(args.output).exists():
        old_json = json.load(open(args.output))
    else:
        old_json = {}

    output_json = merge_jsons(old_json, output_json, args, graph_paths)
            
    json.dump(output_json, open(args.output, mode='w'), indent=4, sort_keys=True)

if __name__ == "__main__":
    check_cwd()

    parser = argparse.ArgumentParser(description="Runs weak scaling test on power-law graphs")
    parser.add_argument('--n', type=int, default=200000)
    parser.add_argument('--num_nodes_min', type=int, default=1)
    parser.add_argument('--num_nodes_max', type=int, default=32)
    parser.add_argument('--num_iters', type=int, default=4)
    parser.add_argument('--kind', type=str, default='openmp')
    parser.add_argument('--output', type=str, default='weak_scaling.json')
    parser.add_argument('--is_production', type=bool, default=False)

    args = parser.parse_args()

    main(args)
