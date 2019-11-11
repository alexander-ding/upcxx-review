""" Generates random power-law graphs
"""
import argparse
import json
import os
import subprocess
from pathlib import Path

from utils import (CODE_PATH, GRAPH_PATH, UTILS_PATH, add_weights_graph,
                   check_cwd, mkdir_if_necessary)


def get_graphs(graphs):
    graph_paths = []
    for graph in graphs:
        folder, filename = graph.split("/")
        graph_path = GRAPH_PATH / folder / "unweighted" / filename
        graph_path_weighted = GRAPH_PATH / folder / "weighted" / filename
        assert graph_path.exists()
        graph_paths.append([graph_path, graph_path_weighted, filename])
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
        command = "CODE_MODE=PRODUCTION {}/bin/upcxx-run -n {} -shared-heap 4G {}".format(os.environ['UPCXX_INSTALL'], num_nodes, (CODE_PATH / "upcxx" / run_command).absolute())
    print(command)
    output = subprocess.check_output(command, shell=True)
    result = float(output.decode("utf-8")[:-1].split("\n")[-1])
    print(result)
    return result

algorithms = ['bfs', 'bellman_ford', 'pagerank', 'connected_components']

def main(args):
    graph_paths = get_graphs(args.graphs)
    
    
    if Path(args.output).exists():
        output_json = json.load(open(args.output))
    else:
        output_json = {}
    for algorithm in algorithms:
        if algorithm not in output_json.keys():
            output_json[algorithm] = {}
        for graph in graph_paths:
            if graph[-1] not in output_json[algorithm].keys():
                output_json[algorithm][graph[-1]] = {}
            num_nodes = args.num_nodes_min
            while num_nodes <= args.num_nodes_max:
                runtime = run(algorithm, graph, args.kind, num_nodes, args.num_iters)
                output_json[algorithm][graph[-1]][num_nodes] = runtime
                num_nodes *= 2
    json.dump(output_json, open(args.output))

if __name__ == "__main__":
    check_cwd()

    parser = argparse.ArgumentParser(description="Generates random power-law graphs")
    parser.add_argument('--graphs', type=str, required=True, nargs="+")
    parser.add_argument('--num_nodes_min', type=int, default=1)
    parser.add_argument('--num_nodes_max', type=int, default=32)
    parser.add_argument('--num_iters', type=int, default=4)
    parser.add_argument('--kind', type=str, default='openmp')
    parser.add_argument('--output', type=str, default='strong_scaling.json')

    args = parser.parse_args()

    main(args)
