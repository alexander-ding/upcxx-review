#ifndef GRAPH_H_
#define GRAPH_H_

#include <iostream>
#include <fstream>
#include <upcxx/upcxx.hpp>

using namespace std;
using namespace upcxx;

class Graph {
    DVector<int> offsets;
    DVector<int> edges;

    

    public:
        Graph();
        void populate(const char *path);
        int num_nodes() { return offsets.size(); };
        int num_edges() { return edges.size();};
        int out_degree(int n);

        vector<int> out_neighbors(int n);
        vector<int> in_neighbors(int n);
};
#include "graph.cpp"
#endif // GRAPH_H_