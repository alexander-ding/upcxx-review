""" Generates random power-law graphs
"""
import argparse
import os
import subprocess
from pathlib import Path

from utils import (GRAPH_PATH, UTILS_PATH, add_weights_graph, check_cwd,
                   mkdir_if_necessary)


def get_graphs(graphs):
    graph_paths = []
    for graph in graphs:
        folder, filename = graph.split("/")
        graph_path = GRAPH_PATH / folder / "unweighted" / filename
        assert graph_path.exists()
        graph_path.append(graph_path)
    return graph_paths

def main(args):
    graph_paths = get_graphs(args.graphs)
    

    num_nodes = args.num_nodes_min
    while num_nodes <= args.num_nodes_max:
        run_tests(num_nodes, graph_path)
        num_nodes *= 2

if __name__ == "__main__":
    check_cwd()

    parser = argparse.ArgumentParser(description="Generates random power-law graphs")
    parser.add_argument('--graphs', type=str, nargs="+")
    parser.add_argument('--num_nodes_min', type=int, default=1)
    parser.add_argument('--num_nodes_max', type=int, default=32)

    args = parser.parse_args()

    main(args)
