#include <cassert>
#include "graph.hpp"

Graph::Graph(char* path) {
    ifstream fin(path);
    int n, m;
    fin >> n >> m;

    int offset, edge; 
    for (int i = 0; i < n; i++) {
        fin >> offset; 
        this->offsets.push_back(offset);
    }

    for (int i = 0; i < m; i++) {
        fin >> edge; // edge from current_node to node edge
        this->edges.push_back(edge);
    }
}

int Graph::out_degree(int n) const {
    assert(n<this->num_nodes());

    if (n == (this->num_nodes()-1)) return this->num_edges()-this->offsets[n];
    return this->offsets[n+1] - this->offsets[n];
}

vector<int> Graph::out_neighbors(int n) const {
    assert(n<this->num_nodes());

    auto begin = this->edges.cbegin()+this->offsets[n];
    vector<int> neighbors(begin, begin+this->out_degree(n));
    return neighbors;
}

vector<int> Graph::in_neighbors(int n) const {
    assert(n<this->num_nodes());

    vector<int> nodes; 
    int current_node = 0;
    int next_offset = (current_node == (this->num_nodes()-1) ? this->num_nodes() : this->offsets[current_node+1]);
    for (int i = 0; i < this->num_edges(); i++) { 
        // edges from current_node to this->edges[i]
        while (i == next_offset) {
            current_node += 1;
            next_offset = (current_node == (this->num_nodes()-1) ? this->num_edges() : this->offsets[current_node+1]);
        }
        if (this->edges[i] == n) {
            nodes.push_back(current_node);
        }
    }
    return nodes;
}