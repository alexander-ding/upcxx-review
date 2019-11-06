#ifndef GRAPH_HPP
#define GRAPH_HPP

#include <vector>
#include <fstream>
#include <upcxx/upcxx.hpp>
#include "utils.hpp"

using namespace std;
using namespace upcxx;

typedef int VertexId;
typedef int EdgeId;

class Graph {
    dist_object<EdgeId*> out_offsets;
    dist_object<VertexId*> out_edges; 

    dist_object<EdgeId*> in_offsets;
    dist_object<VertexId*> in_edges;
        
    public:
        Graph(char* path);

        VertexId rank_start;
        VertexId rank_end;
        
        VertexId num_nodes;
        VertexId num_nodes_local;
        EdgeId num_edges;
        EdgeId num_out_edges_local; 
        EdgeId num_in_edges_local;

        int vertex_rank(const VertexId n);
        EdgeId in_degree(const VertexId n);
        EdgeId out_degree(const VertexId n);

        VertexId* in_neighbors(const VertexId n);
        VertexId* out_neighbors(const VertexId n);
};

Graph::Graph(char *path) : in_offsets(newA(EdgeId, 0)), in_edges(newA(VertexId, 0)), out_offsets(newA(EdgeId, 0)), out_edges(newA(VertexId, 0)) {
    ifstream fin(path);
    VertexId n; EdgeId m;
    fin >> n >> m;
    num_nodes = n; num_edges = m;

    // get rid of default values
    free(*out_offsets); free(*out_edges); 
    free(*in_offsets); free(*in_edges); 
    
    rank_start = int(n / rank_n() * rank_me());
    if (rank_me() == rank_n() - 1) {
        rank_end = n;
    } else {
        rank_end = int(n / rank_n() * (rank_me()+1));
    }

    num_nodes_local = rank_end-rank_start;

    *out_offsets = newA(EdgeId, num_nodes_local);
    *in_offsets = newA(EdgeId, num_nodes_local);
    
    int offset, edge; 
    int offset_start;
    int offset_end = -1;

    // loop through and ignore all non-local nodes
    for (int i = 0; i < n; i++) {
        fin >> offset;
        if (i == rank_start) {
            offset_start = offset;
        }
        if (i >= rank_start && i < rank_end)
            (*out_offsets)[i-rank_start] = offset-offset_start;
        if (i == rank_end)
            offset_end = offset;
    }
    if (offset_end == -1) // if unset, set it
        offset_end = m;

    num_out_edges_local = offset_end - offset_start;
    *out_edges = newA(VertexId, num_out_edges_local);

    for (int i = 0; i < m; i++) {
        fin >> edge;
        if (i >= offset_start && i < offset_end) {
            (*out_edges)[i-offset_start] = edge;
        }
    }

    // loop through and ignore all non-local nodes
    for (int i = 0; i < n; i++) {
        fin >> offset;
        if (i == rank_start) {
            offset_start = offset;
        }
        if (i >= rank_start && i < rank_end)
            (*in_offsets)[i-rank_start] = offset-offset_start;
        if (i == rank_end)
            offset_end = offset;
    }
    if (offset_end == -1) // if unset, set it
        offset_end = m;

    num_in_edges_local = offset_end - offset_start;
    *in_edges = newA(EdgeId, num_in_edges_local);
    for (int i = 0; i < m; i++) {
        fin >> edge;
        if (i >= offset_start && i < offset_end) {
            (*in_edges)[i-offset_start] = edge;
        }
    }
}

int Graph::vertex_rank(const VertexId n) {
    return int(n / (num_nodes / rank_n()));
}

EdgeId Graph::in_degree(const VertexId n)  {
    assert((n >= rank_start) && (n < rank_end));
    if ((n - rank_start) == (num_nodes_local-1)) {
        return num_in_edges_local - (*in_offsets)[num_nodes_local-1];
    }
    return (*in_offsets)[(n-rank_start)+1] - (*in_offsets)[n-rank_start];
}

EdgeId Graph::out_degree(const int n)  {
    assert((n >= rank_start) && (n < rank_end));
    if ((n - rank_start) == (num_nodes_local-1)) {
        return num_out_edges_local - (*out_offsets)[num_nodes_local-1];
    }
    return (*out_offsets)[(n-rank_start)+1] - (*out_offsets)[n-rank_start];
}

VertexId* Graph::in_neighbors(const int n) {
    assert((n >= rank_start) && (n < rank_end));
    return (*in_edges) + (*in_offsets)[n-rank_start];
}

VertexId* Graph::out_neighbors(const int n) {
    assert((n >= rank_start) && (n < rank_end));
    return (*out_edges) + (*in_offsets)[n-rank_start];
}

#endif