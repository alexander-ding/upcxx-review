#ifndef GRAPH_H_
#define GRAPH_H_

#include <vector>
#include <iostream>
#include <fstream>

using namespace std;

class Graph {
    vector<int> offsets;
    vector<int> edges;
    vector<vector<int>> edges_to;
    vector<vector<int>> edges_from;

    public:
        int num_nodes;
        int num_edges;
        Graph(char *path);

        int out_degree(int n);
        vector<int> out_neighbors(int n);

        vector<int> in_neighbors(int n);
};

#endif // GRAPH_H_