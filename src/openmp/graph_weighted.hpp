#ifndef GRAPH_H_
#define GRAPH_H_

#include <vector>
#include <iostream>
#include <fstream>
#include <cassert>
#include <climits>
#include "utils.hpp"

using namespace std;

typedef long VertexId;
typedef long EdgeId; 
typedef EdgeId Weight;

const long INF = LONG_MAX;

class Graph {
    EdgeId* out_offsets;
    VertexId* out_edges;
    Weight* out_weights;

    EdgeId* in_offsets;
    VertexId* in_edges;
    Weight* in_weights;

    public:
        Graph(char *path);

        VertexId num_nodes;
        EdgeId num_edges;
        
        EdgeId out_degree(VertexId n) const;
        EdgeId in_degree(VertexId n) const;

        VertexId* out_neighbors(VertexId n) const;
        VertexId* in_neighbors(VertexId n) const;

        Weight* out_weights_neighbors(VertexId n) const;
        Weight* in_weights_neighbors(VertexId n) const;
};

Graph::Graph(char* path) {
    if (!file_exists(path)) {
        cout << "Graph file does not exist" << endl;
        abort();
    }
    ifstream fin(path);
    VertexId n;
    EdgeId m;
    fin >> n >> m;
    this->num_nodes = n;
    this->num_edges = m;

    out_offsets = newA(EdgeId, n);
    in_offsets = newA(EdgeId, n);

    out_edges = newA(EdgeId, m);
    in_edges = newA(VertexId, m);

    out_weights = newA(Weight, m);
    in_weights = newA(Weight, m);

    EdgeId offset;
    VertexId edge;
    Weight weight;
    for (EdgeId i = 0; i < n; i++) {
        fin >> offset; 
        out_offsets[i] = offset;
    }

    for (VertexId i = 0; i < m; i++) {
        fin >> edge >> weight; // edge from current_node to node edge
        out_edges[i] = edge;
        out_weights[i] = weight;
    }

    for (EdgeId i = 0; i < n; i++) {
        fin >> offset; 
        in_offsets[i] = offset;
    }

    for (VertexId i = 0; i < m; i++) {
        fin >> edge >> weight; // edge from current_node to node edge
        in_edges[i] = edge;
        in_weights[i] = weight;
    }
}

EdgeId Graph::out_degree(VertexId n) const {
    if (n == (this->num_nodes-1)) return this->num_edges-this->out_offsets[n];
    return this->out_offsets[n+1] - this->out_offsets[n];
}

EdgeId Graph::in_degree(VertexId n) const {
    if (n == (this->num_nodes-1)) return this->num_edges-this->in_offsets[n];
    return this->in_offsets[n+1] - this->in_offsets[n];
}

VertexId* Graph::out_neighbors(VertexId n) const {
    return this->out_edges+this->out_offsets[n];
}

VertexId* Graph::in_neighbors(VertexId n) const {
    return this->in_edges+this->in_offsets[n];
}

Weight* Graph::out_weights_neighbors(VertexId n) const {
    return this->out_weights+this->out_offsets[n];
}

Weight* Graph::in_weights_neighbors(VertexId n) const {
    return this->in_weights+this->in_offsets[n];
}

#endif // GRAPH_H_