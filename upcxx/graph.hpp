#ifndef GRAPH_HPP
#define GRAPH_HPP

#include <vector>
#include <upcxx/upcxx.hpp>

using namespace std;
using namespace upcxx;

class Graph {
    using dobj_vector_t = dist_object<vector<int>>;
    
    private: 
        void _populate(vector<vector<int>> & edges, bool in);
    public:
    dobj_vector_t in_offsets;
    dobj_vector_t in_edges;
    dobj_vector_t out_offsets;
    dobj_vector_t out_edges; 
        int rank_start;
        int rank_end;
        // a sequential operation to load the graph. Returns 
        // upon completion
        Graph(char* path);

        int in_degree(const int n) const;
        int out_degree(const int n) const;

        vector<int> in_neighbors(const int n) const;
        vector<int> out_neighbors(const int n) const;
};

#endif