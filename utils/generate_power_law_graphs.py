import os
import subprocess
from configparser import ConfigParser
from pathlib import Path

from GraphParser import convert_to_weighted, mkdir_if_necessary


def generate_graph(num_nodes, output_dir):
    output_name = output_dir / f"{num_nodes}_temp.txt"
    output_name_should = output_dir / f"{num_nodes}.txt"
    command = f"bash -c './utils/ligra/utils/rMatGraph {num_nodes} {output_name}'"
    output = subprocess.check_output(command, shell=True)
    return output_name, output_name_should

def read_graph(from_p):
    with open(from_p) as fin:
        fin.readline() # ignore header

        n = int(fin.readline()[:-1])
        m = int(fin.readline()[:-1])
        offsets = [0 for _ in range(n)]
        nodes = [[] for _ in range(n)]
        for i in range(n):
            offset = int(fin.readline()[:-1])
            offsets[i] = offset
        
        node_from = 0
        for i in range(m):
            if node_from != m - 1 and i > offsets[node_from+1]:
                node_from += 1
            node_to = int(fin.readline()[:-1])
            nodes[node_from].append(node_to)

    return nodes

def invert_nodes(nodes):
    nodes_inverse = [[] for _ in range(len(nodes))]
    for node_from in range(len(nodes)):
        for node_to in nodes[node_from]:
            nodes_inverse[node_to].append(node_from)
    del nodes
    return nodes_inverse

def write_graph(fout, nodes):
    offsets = []
    current_offset = 0
    for i in range(len(nodes)):
        offsets.append(current_offset)
        current_offset += len(nodes[i])
    for i in range(len(nodes)):
        fout.write(f"{offsets[i]}\n")
    for node in nodes:
        for node_to in node:
            fout.write(f"{node_to}\n")

def convert_graph(from_p, to_p):
    nodes = read_graph(from_p)
    nodes = invert_nodes(nodes)
    with open(to_p, 'w') as fout:
        with open(from_p) as fin:
            fin.readline() # ignore header
            n = int(fin.readline()[:-1])
            m = int(fin.readline()[:-1])
            fout.write(f"{n}\n{m}\n")
            for i in range(n):
                offset = int(fin.readline()[:-1])
                fout.write(f"{offset}\n")
            for i in range(m):
                edge = int(fin.readline()[:-1])
                fout.write(f"{edge}\n")
            
            write_graph(fout, nodes)

    os.remove(from_p)
    mkdir_if_necessary(to_p.parent.parent / "weighted")
    convert_to_weighted(to_p, to_p.parent.parent / "weighted" / to_p.name)

def init():
    config = ConfigParser()
    config.read("config.ini")
    global GRAPH_PATH

    GRAPH_PATH = Path(config.get("DEFAULT", "RealGraphPath")).parent / "powerlaw"

GRAPH_PATH = None

def main():
    init()

    mkdir_if_necessary(GRAPH_PATH)
    mkdir_if_necessary(GRAPH_PATH / "unweighted")
    i = 10000
    i_max = 32 * i
    while i <= i_max:
        graph_p, should_be_p = generate_graph(i, GRAPH_PATH / "unweighted")
        convert_graph(graph_p, should_be_p)
        i *= 2

if __name__ == "__main__":
    cws = os.getcwd()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if os.path.join(cws, "utils") != dir_path:
        raise Exception("generate_power_law_graphs.py must be run from the top-level directory of the repository")
    main()
