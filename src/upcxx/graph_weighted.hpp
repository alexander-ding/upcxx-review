#ifndef GRAPH_HPP
#define GRAPH_HPP

#include <vector>
#include <upcxx/upcxx.hpp>
#include <fstream>

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
    int offset, edge, weight; 
    int start_offset;
    int end_offset = -1;

    vector<int> in_offsets_vector(rank_end-rank_start);
    vector<int> in_edges_vector;
    vector<int> in_weights_vector;
    vector<vector<int>> in_edges_matrix(rank_end-rank_start);
    vector<vector<int>> in_weights_matrix(rank_end-rank_start);
    vector<int> out_offsets_vector(rank_end-rank_start);
    vector<int> out_edges_vector;
    vector<int> out_weights_vector;

    for (int i = 0; i < n; i++) {
        fin >> offset;
        temp_offsets[i] = offset;
        if (i == rank_start) {
            start_offset = offset;
        }
        if (i >= rank_start && i < rank_end)
            out_offsets_vector[i-rank_start] = offset-start_offset;
        if (i == rank_end)
            end_offset = offset;
    }
    if (end_offset == -1) // if unset, set it
        end_offset = m;

    int current_node_end = -1;
    int current_node = -1;
    for (int i = 0; i < m; i++) {
        if (i >= current_node_end) {
            current_node += 1;
            current_node_end = (current_node == (n-1))  ? m : temp_offsets[current_node+1];
        }

        fin >> edge >> weight;
        if (i >= start_offset && i < end_offset) {
            out_edges_vector.push_back(edge);
            out_weights_vector.push_back(weight);
        }

        // if the node_to is within desired range
        if (edge >= rank_start && edge < rank_end) {
            in_edges_matrix[edge-rank_start].push_back(current_node);
            in_weights_matrix[edge-rank_start].push_back(weight);
        }    
    }

    int offset_temp = 0;
    for (int i = 0; i < (rank_end-rank_start); i++) {
        in_offsets_vector[i] = offset_temp;
        for (int j = 0; j < in_edges_matrix[i].size(); j++) {
            in_edges_vector.push_back(in_edges_matrix[i][j]);
            in_weights_vector.push_back(in_weights_matrix[i][j]);
        }
        offset_temp += in_edges_matrix[i].size();
    }

    *in_offsets = in_offsets_vector;
    *in_edges = in_edges_vector;
    *in_weights = in_weights_vector;

    *out_offsets = out_offsets_vector;
    *out_edges = out_edges_vector;
    *out_weights = out_weights_vector;
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


#endif

