#ifndef GRAPH_H_
#define GRAPH_H_

#include <vector>
#include <iostream>
#include <fstream>

using namespace std;

class Graph {
    vector<int> offsets;
    vector<int> edges;

    public:
        Graph(char *path);
        int num_nodes() const { return offsets.size(); };
        int num_edges() const { return edges.size(); };
        int out_degree(int n) const;
        vector<int> out_neighbors(int n) const;

        vector<int> in_neighbors(int n) const;
};

#endif // GRAPH_H_