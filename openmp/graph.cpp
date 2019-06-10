#include <cassert>
#include "graph.hpp"

Graph::Graph(char* path) {
    ifstream fin(path);
    int n, m;
    fin >> n >> m;

    vector<int> temp_offsets(n);
    vector<int> temp_edges(m);
    int offset, edge; 
    for (int i = 0; i < n; i++) {
        fin >> offset; 
        temp_offsets[i] = offset;
    }

    for (int i = 0; i < m; i++) {
        fin >> edge; // edge from current_node to node edge
        temp_edges[i] = edge;
    }

    vector<vector<int>> edges_out(n); 
    int index = 0;
    for (int i = 0; i < n; i++) {
        int limit = (i==(n-1)) ? (m-index) : (temp_offsets[i+1]-temp_offsets[i]);

        for (int j = 0; j < limit; j++) {
            edges_out[i].push_back(temp_edges[index]);
            index++;
        }
    }

    vector<vector<int>> edges_in(n);
    for (int i = 0; i < edges_out.size(); i++) {
        for (int j = 0; j < edges_out[i].size(); j++) {
            edges_in[edges_out[i][j]].push_back(i);
        }
    }
    _populate(edges_in, true, n);
    _populate(edges_out, false, n);
}

void Graph::_populate(vector<vector<int>> & edges, bool in, int n) {
    vector<int> local_offsets(n);
    vector<int> local_edges;
    local_offsets[0] = 0;
    for (int i = 0; i < n; i++) {
        int offset = edges[i].size();
        if (i != (n-1))
            local_offsets[i+1] = local_offsets[i]+offset;
        for (int j = 0; j < offset; j++) {
            local_edges.push_back(edges[i][j]);
        }
    }
    
    if (in) {
        in_offsets = local_offsets;
        in_edges = local_edges;
    } else {
        out_offsets = local_offsets;
        out_edges = local_edges;
    }
}

int Graph::out_degree(int n) const {
    assert(n<this->num_nodes());

    if (n == (this->num_nodes()-1)) return this->num_edges()-this->out_offsets[n];
    return this->out_offsets[n+1] - this->out_offsets[n];
}

int Graph::in_degree(int n) const {
    assert(n<this->num_nodes());

    if (n == (this->num_nodes()-1)) return this->num_edges()-this->in_offsets[n];
    return this->in_offsets[n+1] - this->in_offsets[n];
}

vector<int> Graph::out_neighbors(int n) const {
    assert(n<this->num_nodes());

    auto begin = this->out_edges.cbegin()+this->out_offsets[n];
    vector<int> neighbors(begin, begin+this->out_degree(n));
    return neighbors;
}

vector<int> Graph::in_neighbors(int n) const {
    assert(n<this->num_nodes());

    auto begin = this->in_edges.cbegin()+this->in_offsets[n];
    vector<int> neighbors(begin, begin+this->in_degree(n));
    return neighbors;
}