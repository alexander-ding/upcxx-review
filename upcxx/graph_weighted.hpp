#ifndef GRAPH_HPP
#define GRAPH_HPP

#include <vector>
#include <upcxx/upcxx.hpp>

using namespace std;
using namespace upcxx;

class Graph {
    using dobj_vector_t = dist_object<vector<int>>;
    dobj_vector_t in_offsets;
    dobj_vector_t in_edges;
    dobj_vector_t in_weights;
    dobj_vector_t out_offsets;
    dobj_vector_t out_edges; 
    dobj_vector_t out_weights;
    private: 
        void _populate(vector<vector<int>> & edges, vector<vector<int>> & weights, bool in);
        int _degree(vector<int> &offsets, const int n, const int total_edges);
        vector<int> _neighbors(vector<int> &offsets, vector<int> &edges, int n);
        vector<int> _weights(vector<int> &offsets, vector<int> &weights, int n);
        
    public:
        int rank_start;
        int rank_end;
        int num_nodes;
        int num_edges;
        // a sequential operation to load the graph. Returns 
        // upon completion
        Graph(char* path);

        int in_degree(const int n);
        int out_degree(const int n);

        vector<int> in_neighbors(const int n);
        vector<int> out_neighbors(const int n);
        vector<int> in_weights_neighbors(const int n);
        vector<int> out_weights_neighbors(const int n);
        
};

#endif