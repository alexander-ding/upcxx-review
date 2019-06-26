import sys
import random
from pathlib import Path
import argparse

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

def main(args):
    n, m, p = args.n_nodes, args.n_edges, args.output
    
    nodes = []
    for i in range(n):
        nodes.append(Node(i))
    if args.weighted:
        for _ in range(m):
            from_node, to_node = generate_tuple(n)
            weight = generate_weight()
            nodes[from_node].edges.append((to_node, weight))
    else:
        for _ in range(m):
            from_node, to_node = generate_tuple(n)
            nodes[from_node].edges.append(to_node)

    with open(Path("graphs") / p, mode='w') as f:
        f.write('{}\n'.format(n))
        f.write('{}\n'.format(m))
        current_e = 0
        for node in nodes:
            f.write('{}\n'.format(current_e))
            current_e += len(node.edges)
        
        for node in nodes:
            for edge in node.edges:
                if args.weighted:
                    f.write('{} {}\n'.format(edge[0], edge[1]))
                else:
                    f.write('{}\n'.format(edge))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate random graphs.')
    parser.add_argument("-V", '--n_nodes', type=int, required=True,
                    help='number of nodes')
    parser.add_argument("-E", "--n_edges", type=int, required=True,
                    help="number of edges")
    parser.add_argument("-W", "--weighted", action="store_true", 
                    help="whether the graph is weighted")
    parser.add_argument("-O", "--output", type=str, required=True,
                    help="where the output graph is stored")
    args = parser.parse_args()
    main(args)