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
    private: 
        vector<int> _neighbors(vector<int> &offsets, vector<int> &edges, int n);
        
    public:
        Graph(char* path);

        VertexId rank_start;
        VertexId rank_end;
        
        VertexId num_nodes;
        VertexId num_nodes_local;
        EdgeId num_edges;
        EdgeId num_out_edges_local; 
        EdgeId num_in_edges_local;

        EdgeId in_degree(const int n);
        EdgeId out_degree(const int n);

        VertexId* in_neighbors(const int n);
        VertexId* out_neighbors(const int n);
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

int Graph::in_degree(const int n)  {
    assert((n >= rank_start) && (n < rank_end));
    if ((n >= rank_start) && (n < rank_end)) {
        if ((n - rank_start) == num_nodes_local) {
            return num_in_edges_local - (*in_offsets)[num_nodes_local-1];
        }
        return (*in_offsets)[(n-rank_start)+1] - (*in_offsets)[n-rank_start];
    }
}

int Graph::out_degree(const int n)  {
    assert((n >= rank_start) && (n < rank_end));
    if ((n >= rank_start) && (n < rank_end)) {
        if ((n - rank_start) == num_nodes_local) {
            return num_out_edges_local - (*out_offsets)[num_nodes_local-1];
        }
        return (*out_offsets)[(n-rank_start)+1] - (*out_offsets)[n-rank_start];
    }
}

vector<int> Graph::in_neighbors(const int n) {
    if ((n >= rank_start) && (n < rank_end)) {
        return _neighbors(*in_offsets, *in_edges, n-rank_start);
    } else {
        int rank =  int(n / (num_nodes / rank_n()));
        auto offsets = in_offsets.fetch(rank).wait();
        auto edges = in_edges.fetch(rank).wait();
        return _neighbors(offsets, edges, n-rank*int(num_nodes / rank_n()));
    }
}

vector<int> Graph::out_neighbors(const int n) {
    if ((n >= rank_start) && (n < rank_end)) {
        return _neighbors(*out_offsets, *out_edges, n-rank_start);
    } else {
        int rank =  int(n / (num_nodes / rank_n()));
        auto offsets = out_offsets.fetch(rank).wait();
        auto edges = out_edges.fetch(rank).wait();
        return _neighbors(offsets, edges, n-rank*int(num_nodes / rank_n()));
    }
}

vector<int> Graph::_neighbors(vector<int> &offsets, vector<int> &edges, int n) {
    auto begin = edges.begin() + offsets[n];
    auto end = (n == (offsets.size()-1)) ? (edges.end()) : (edges.begin() + offsets[n+1]);
    vector<int> to_return(begin, end);
    return to_return;
}

#endif