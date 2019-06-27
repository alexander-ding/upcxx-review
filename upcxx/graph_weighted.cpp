#include "graph_weighted.hpp"
#include <fstream>
#include <vector>
#include "upcxx/upcxx.hpp"

using namespace std;
using namespace upcxx;

Graph::Graph(char *path) : out_offsets(vector<int>()), out_edges(vector<int>()), out_weights(vector<int>()), in_offsets(vector<int>()), in_edges(vector<int>()), in_weights(vector<int>()) {
    ifstream fin(path);
    int n, m;
    fin >> n >> m;
    
    rank_start = int(n / rank_n() * rank_me());
    if (rank_me() == rank_n() - 1) {
        rank_end = n;
    } else {
        rank_end = int(n / rank_n() * (rank_me()+1));
    }
    num_nodes = n;
    num_edges = m;

    vector<int> temp_offsets(n);
    vector<int> temp_edges(m);
    vector<int> temp_weights(m);
    int offset, edge, weight; 
    for (int i = 0; i < n; i++) {
        fin >> offset;
        temp_offsets[i] = offset;
    }

    for (int i = 0; i < m; i++) {
        fin >> edge >> weight;
        temp_edges[i] = edge;
        temp_weights[i] = weight;
    }

    vector<vector<int>> edges_out(n); 
    vector<vector<int>> weights_out(n);
    int index = 0;
    for (int i = 0; i < n; i++) {
        int limit = (i==(n-1)) ? (m-index) : (temp_offsets[i+1]-temp_offsets[i]);
        for (int j = 0; j < limit; j++) {
            edges_out[i].push_back(temp_edges[index]);
            weights_out[i].push_back(temp_weights[index]);
            index++;
        }
    }

    vector<vector<int>> edges_in(n);
    vector<vector<int>> weights_in(n);
    for (int i = 0; i < edges_out.size(); i++) {
        for (int j = 0; j < edges_out[i].size(); j++) {
            int from = edges_out[i][j]; // i is to
            edges_in[edges_out[i][j]].push_back(i);
            weights_in[from].push_back(weights_out[i][j]);
        }
    }

    _populate(edges_in, weights_in, true);
    _populate(edges_out, weights_out, false);
}

void Graph::_populate(vector<vector<int>> & edges, vector<vector<int>> & weights, bool in) {
    vector<int> local_offsets(rank_end-rank_start);
    vector<int> local_edges;
    vector<int> local_weights;
    local_offsets[0] = 0;
    for (int i = rank_start; i < rank_end; i++) {
        int offset = edges[i].size();
        if (i != (rank_end-1))
            local_offsets[i-rank_start+1] = local_offsets[i-rank_start]+offset;
        for (int j = 0; j < offset; j++) {
            local_edges.push_back(edges[i][j]);
            local_weights.push_back(weights[i][j]);
        }
    }
    
    if (in) {
        *in_offsets = local_offsets;
        *in_edges = local_edges;
        *in_weights = local_weights;
    } else {
        *out_offsets = local_offsets;
        *out_edges = local_edges;
        *out_weights = local_weights;
    }
}

int Graph::in_degree(const int n)  {
    if ((n >= rank_start) && (n < rank_end)) {
        return _degree(*in_offsets, n-rank_start, in_edges->size());
    } else {
        int rank =  int(n / (num_nodes / rank_n()));
        auto offsets = in_offsets.fetch(rank).wait();
        auto edges = in_edges.fetch(rank).wait();
        return _degree(offsets, n-rank*int(num_nodes / rank_n()), edges.size());
    }
}

int Graph::out_degree(const int n)  {
    if ((n >= rank_start) && (n < rank_end)) {
        return _degree(*out_offsets, n-rank_start, out_edges->size());
    } else {
        int rank =  int(n / (num_nodes / rank_n()));
        auto offsets = out_offsets.fetch(rank).wait();
        auto edges = out_edges.fetch(rank).wait();
        return _degree(offsets, n-rank*int(num_nodes / rank_n()), edges.size());
    }
}


int Graph::_degree(vector<int> &offsets, const int n, const int total_edges) {
    if (n == (offsets.size()-1)) {
        return total_edges - offsets[offsets.size()-1];
    }
    return offsets[n+1] - offsets[n];
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
        int rank = int(n / (num_nodes / rank_n()));
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

vector<int> Graph::in_weights_neighbors(const int n) {
    if ((n >= rank_start) && (n < rank_end)) {
        return _weights(*in_offsets, *in_weights, n-rank_start);
    } else {
        int rank = int(n / (num_nodes / rank_n()));
        auto offsets = in_offsets.fetch(rank).wait();
        auto weights = in_weights.fetch(rank).wait();
        return _weights(offsets, weights, n-rank*int(num_nodes / rank_n()));
    }
}

vector<int> Graph::out_weights_neighbors(const int n) {
    if ((n >= rank_start) && (n < rank_end)) {
        return _weights(*out_offsets, *out_weights, n-rank_start);
    } else {
        int rank = int(n / (num_nodes / rank_n()));
        auto offsets = out_offsets.fetch(rank).wait();
        auto weights = out_weights.fetch(rank).wait();
        return _weights(offsets, weights, n-rank*int(num_nodes / rank_n()));
    }
}

vector<int> Graph::_weights(vector<int> &offsets, vector<int> &weights, int n) {
    auto begin = weights.begin() + offsets[n];
    auto end = (n == (offsets.size()-1)) ? (weights.end()) : (weights.begin() + offsets[n+1]);
    vector<int> to_return(begin, end);
    return to_return;
}
