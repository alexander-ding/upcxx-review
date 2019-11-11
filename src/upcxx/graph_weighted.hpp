#ifndef GRAPH_HPP
#define GRAPH_HPP

// TODO: fully update

#include <vector>
#include <fstream>
#include <upcxx/upcxx.hpp>
#include "utils.hpp"

using namespace std;
using namespace upcxx;

typedef long VertexId;
typedef long EdgeId;
typedef long Weight;

const long INF = LONG_MAX;

class Graph {
    global_ptr<EdgeId> out_offsets_dist;
    EdgeId* out_offsets;
    global_ptr<VertexId> out_edges_dist; 
    VertexId* out_edges;
    global_ptr<Weight> out_weights_dist;
    Weight* out_weights;

    global_ptr<EdgeId> in_offsets_dist;
    EdgeId* in_offsets;
    global_ptr<VertexId> in_edges_dist;
    VertexId* in_edges;
    global_ptr<Weight> in_weights_dist;
    Weight* in_weights;
        
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

        global_ptr<VertexId> in_neighbors(const VertexId n);
        global_ptr<VertexId> out_neighbors(const VertexId n);
        global_ptr<Weight> in_weights_neighbors(const VertexId n);
        global_ptr<Weight> out_weights_neighbors(const VertexId n);

        inline VertexId rank_start_node(const int n) { 
            return int(num_nodes / rank_n() * n);
        }
        inline VertexId rank_end_node(const int n) {
            return (n == rank_n() - 1) ? (num_nodes) : (int(num_nodes / rank_n() * (n+1)));
        }
        inline VertexId rank_num_nodes(const int n) {
            return rank_end_node(n) - rank_start_node(n);
        }
};

Graph::Graph(char *path) {
    if (!file_exists(path)) {
        if (rank_me() == 0)
            cout << "Graph file does not exist" << endl;
        abort();
    }
    ifstream fin(path);
    VertexId n; EdgeId m;
    fin >> n >> m;
    num_nodes = n; num_edges = m;

    rank_start = int(n / rank_n() * rank_me());
    if (rank_me() == rank_n() - 1) {
        rank_end = n;
    } else {
        rank_end = int(n / rank_n() * (rank_me()+1));
    }

    num_nodes_local = rank_end-rank_start;

    out_offsets_dist = new_array<EdgeId>(num_nodes_local);
    in_offsets_dist = new_array<EdgeId>(num_nodes_local);
    out_offsets = out_offsets_dist.local();
    in_offsets = in_offsets_dist.local();
    
    EdgeId offset;
    VertexId edge;
    Weight weight; 
    EdgeId offset_start;
    EdgeId offset_end = -1;

    // loop through and ignore all non-local nodes
    for (VertexId i = 0; i < n; i++) {
        fin >> offset;
        if (i == rank_start) {
            offset_start = offset;
        }
        if (i >= rank_start && i < rank_end)
            out_offsets[i-rank_start] = offset-offset_start;
        if (i == rank_end)
            offset_end = offset;
    }
    if (offset_end == -1) // if unset, set it
        offset_end = m;

    num_out_edges_local = offset_end - offset_start;
    out_edges_dist = new_array<VertexId>(num_out_edges_local);
    out_edges = out_edges_dist.local();
    out_weights_dist = new_array<Weight>(num_out_edges_local);
    out_weights = out_weights_dist.local();

    for (EdgeId i = 0; i < m; i++) {
        fin >> edge >> weight;
        if (i >= offset_start && i < offset_end) {
            out_edges[i-offset_start] = edge;
            out_weights[i-offset_start] = weight;
        }
    }

    // loop through and ignore all non-local nodes
    for (VertexId i = 0; i < n; i++) {
        fin >> offset;
        if (i == rank_start) {
            offset_start = offset;
        }
        if (i >= rank_start && i < rank_end)
            in_offsets[i-rank_start] = offset-offset_start;
        if (i == rank_end)
            offset_end = offset;
    }
    if (offset_end == -1) // if unset, set it
        offset_end = m;

    num_in_edges_local = offset_end - offset_start;
    in_edges_dist = new_array<VertexId>(num_in_edges_local);
    in_edges = in_edges_dist.local();
    in_weights_dist = new_array<Weight>(num_in_edges_local);
    in_weights = in_weights_dist.local();
    for (EdgeId i = 0; i < m; i++) {
        fin >> edge >> weight;
        if (i >= offset_start && i < offset_end) {
            in_edges[i-offset_start] = edge;
            in_weights[i-offset_start] = weight;
        }
    }
}

int Graph::vertex_rank(const VertexId n) {
    return int(n / (num_nodes / rank_n()));
}

EdgeId Graph::in_degree(const VertexId n)  {
    assert((n >= rank_start) && (n < rank_end));
    if ((n - rank_start) == (num_nodes_local-1)) {
        return num_in_edges_local - in_offsets[num_nodes_local-1];
    }
    return in_offsets[(n-rank_start)+1] - in_offsets[n-rank_start];
}

EdgeId Graph::out_degree(const VertexId n)  {
    assert((n >= rank_start) && (n < rank_end));
    if ((n - rank_start) == (num_nodes_local-1)) {
        return num_out_edges_local - out_offsets[num_nodes_local-1];
    }
    return out_offsets[(n-rank_start)+1] - out_offsets[n-rank_start];
}

global_ptr<VertexId> Graph::in_neighbors(const VertexId n) {
    assert((n >= rank_start) && (n < rank_end));
    return in_edges_dist + in_offsets[n-rank_start];
}

global_ptr<VertexId> Graph::out_neighbors(const VertexId n) {
    assert((n >= rank_start) && (n < rank_end));
    return out_edges_dist + out_offsets[n-rank_start];
}

global_ptr<Weight> Graph::in_weights_neighbors(const Weight n) {
    assert((n >= rank_start) && (n < rank_end));
    return in_weights_dist + in_offsets[n-rank_start];
}

global_ptr<Weight> Graph::out_weights_neighbors(const Weight n) {
    assert((n >= rank_start) && (n < rank_end));
    return out_weights_dist + out_offsets[n-rank_start];
}

#endif