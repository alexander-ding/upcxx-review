import sys
import random

class Node:
    def __init__(self, id, edges=[]):
        self.id = id
        self.edges = []

def generate_tuple(n):
    return (random.randint(0,n-1), random.randint(0,n-1))

def main():
    if len(sys.argv) != 4:
        print("Usage: python generate_graph.py num_nodes num_edges outpath")
    n, m, p = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
    nodes = []
    for i in range(n):
        nodes.append(Node(i))

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
                f.write('{}\n'.format(edge))

if __name__ == "__main__":
    main()