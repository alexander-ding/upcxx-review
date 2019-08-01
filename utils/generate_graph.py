import sys
import random
import os
from pathlib import Path
from configparser import ConfigParser
from GraphParser import mkdir_if_necessary

class Node:
    def __init__(self, id, edges=[]):
        self.id = id
        self.edges = []

def generate_tuple(n):
    return (random.randint(0,n-1), random.randint(0,n-1))

def generate_weight(min=0, max=500, neg_rate=0.01):
    n = random.random()
    m = random.randint(min, max)
    if n < neg_rate:
        return -m
    else:
        return m

def generate_graph(p, n, m, weighted, neg_rate=0.01):
    nodes = []
    for i in range(n):
        nodes.append(Node(i))
    if weighted:
        for _ in range(m):
            from_node, to_node = generate_tuple(n)
            weight = generate_weight(neg_rate=neg_rate)
            nodes[from_node].edges.append((to_node, weight))
    else:
        for _ in range(m):
            from_node, to_node = generate_tuple(n)
            nodes[from_node].edges.append(to_node)

    with open(p, mode='w') as f:
        f.write('{}\n'.format(n))
        f.write('{}\n'.format(m))
        current_e = 0
        for node in nodes:
            f.write('{}\n'.format(current_e))
            current_e += len(node.edges)
        
        for node in nodes:
            for edge in node.edges:
                if weighted:
                    f.write('{} {}\n'.format(edge[0], edge[1]))
                else:
                    f.write('{}\n'.format(edge))

def parse_list(s, t=int):
    return [t(v) for v in s.split(" ")]

def main():
    config = ConfigParser()
    config.read("config.ini")
    
    base_p = Path(config.get("DEFAULT", "RandomGraphPath"))
    node_sizes = parse_list(config.get("DEFAULT", "NodeSizes"))
    edge_proportions = parse_list(config.get("DEFAULT", "EdgeProportions"))
    neg_rates = parse_list(config.get("DEFAULT", "NegRates"), float)
    copies = int(config.get("DEFAULT", "Copies"))

    mkdir_if_necessary(base_p.parent)
    mkdir_if_necessary(base_p)
    
    # unweighted
    for node_size in node_sizes:
        for edge_proportion in edge_proportions:
            unweighted_base = base_p / "unweighted"
            mkdir_if_necessary(unweighted_base)
            for i in range(copies):
                p = unweighted_base / "{}_{}_{}.txt".format(node_size, edge_proportion, i)
                # skip generated files
                if p.exists():
                    continue
                generate_graph(p, node_size, node_size*edge_proportion, False)
    
    # weighted
    for node_size in node_sizes:
        for edge_proportion in edge_proportions:
            weighted_base = base_p / "weighted"
            mkdir_if_necessary(weighted_base)
            for neg_rate in neg_rates:
                for i in range(copies):
                    p = weighted_base / "{}_{}_{}_{}.txt".format(node_size, edge_proportion, neg_rate, i)
                    # skip generated files
                    if p.exists():
                        continue
                    generate_graph(p, node_size, node_size * edge_proportion, True, neg_rate)

if __name__ == "__main__":
    cws = os.getcwd()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if os.path.join(cws, "utils") != dir_path:
        raise Exception("generate_graph.py must be run from the top-level directory of the repository")
        
    main()