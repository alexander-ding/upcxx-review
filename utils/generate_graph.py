""" Generates random power-law graphs
"""
import argparse
import os
import subprocess
from pathlib import Path

from utils import (GRAPH_PATH, UTILS_PATH, add_weights_graph, check_cwd,
                   mkdir_if_necessary)


def generate_graph(num_nodes, output_dir):
    temporary_graph = output_dir / f"{num_nodes}_temp.txt"
    command = f"bash -c '{UTILS_PATH}/rMatGraph -s {num_nodes} {temporary_graph}'"
    print(command)
    output = subprocess.check_output(command, shell=True)

    unweighted_graph = output_dir / "unweighted" / f"{num_nodes}.txt"
    convert_graph(temporary_graph, unweighted_graph)

    weighted_graph = output_dir / "weighted" / f"{num_nodes}.txt"
    add_weights_graph(unweighted_graph, weighted_graph)
    
    os.remove(temporary_graph)

def convert_graph(input_dir, output_dir):
    with open(output_dir, 'w') as fout:
        with open(input_dir) as fin:
            for i, line in enumerate(fin):
                if i != 0:
                    fout.write(line)
        with open(input_dir) as fin:
            for i, line in enumerate(fin):
                if i >= 3: # ignore header, n, and m
                    fout.write(line)

def main(args):
    graph_path = GRAPH_PATH / "powerlaw"
    mkdir_if_necessary(graph_path)
    mkdir_if_necessary(graph_path / "unweighted")
    mkdir_if_necessary(graph_path / "weighted")

    num_nodes = args.num_nodes_min
    while num_nodes <= args.num_nodes_max:
        generate_graph(num_nodes*args.n, graph_path)
        num_nodes *= 2

if __name__ == "__main__":
    check_cwd()

    parser = argparse.ArgumentParser(description="Generates random power-law graphs")
    parser.add_argument('--n', type=int, default=10000000)
    parser.add_argument('--num_nodes_min', type=int, default=1)
    parser.add_argument('--num_nodes_max', type=int, default=32)

    args = parser.parse_args()

    main(args)
