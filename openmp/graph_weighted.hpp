#ifndef GRAPH_H_
#define GRAPH_H_

#include <vector>
#include <iostream>
#include <fstream>

using namespace std;

class Graph {
    vector<int> in_offsets;
    vector<int> in_edges;
    vector<int> in_weights;

    vector<int> out_offsets;
    vector<int> out_edges;
    vector<int> out_weights;
    

    private:
        void _populate(vector<vector<int>> & edges, vector<vector<int>> & weights, bool in, int n);

    public:
        Graph(char *path);
        int num_nodes() const { return out_offsets.size(); };
        int num_edges() const { return out_edges.size(); };
        int out_degree(int n) const;
        int in_degree(int n) const;

        vector<int> out_neighbors(int n) const;
        vector<int> in_neighbors(int n) const;

        vector<int> out_weights_neighbors(int n) const;
        vector<int> in_weights_neighbors(int n) const;
};

#endif // GRAPH_H_