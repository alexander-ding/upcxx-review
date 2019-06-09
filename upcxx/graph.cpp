#include "graph.hpp"
#include <fstream>
#include <vector>
#include "upcxx/upcxx.hpp"

using namespace std;
using namespace upcxx;

Graph::Graph(char *path) : out_offsets(vector<int>()), out_edges(vector<int>()), in_offsets(vector<int>()), in_edges(vector<int>()) {
    ifstream fin(path);
    int n, m;
    fin >> n >> m;
    
    rank_start = int(n / rank_n() * rank_me());
    if (rank_me() == rank_n() - 1) {
        rank_end = n;
    } else {
        rank_end = int(n / rank_n() * (rank_me()+1));
    }

    vector<int> temp_offsets(n);
    vector<int> temp_edges(m);
    int offset, edge; 
    for (int i = 0; i < n; i++) {
        fin >> offset;
        temp_offsets[i] = offset;
    }

    for (int i = 0; i < m; i++) {
        fin >> edge;
        temp_edges[i] = edge;
    }

    vector<vector<int>> edges_out(n); 
    int index = 0;
    for (int i = 0; i < n; i++) {
        int limit = (i==(n-1)) ? (m-index) : (temp_offsets[i+1]-temp_offsets[i]);
        for (int j = 0; j < limit; j++) {
            edges_out[i].push_back(temp_edges[index]);
            index++;
        }
    }

    vector<vector<int>> edges_in(n);
    for (int i = 0; i < edges_out.size(); i++) {
        for (int j = 0; j < edges_out[i].size(); j++) {
            edges_in[edges_out[i][j]].push_back(i);
        }
    }

    _populate(edges_in, true);
    _populate(edges_out, false);
}

void Graph::_populate(vector<vector<int>> & edges, bool in) {
    vector<int> local_offsets(rank_end-rank_start);
    vector<int> local_edges;
    local_offsets[0] = 0;
    for (int i = rank_start; i < rank_end; i++) {
        int offset = edges[i].size();
        if (i != (rank_end-1))
            local_offsets[i-rank_start+1] = offset;
        for (int j = 0; j < offset; j++) {
            local_edges.push_back(edges[i][j]);
        }
    }
    
    if (in) {
        *in_offsets = local_offsets;
        *in_edges = local_edges;
    } else {
        *out_offsets = local_offsets;
        *out_edges = local_edges;
    }
}