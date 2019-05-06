#include <cassert>
#include "graph.hpp"
#include "upcxx/upcxx.hpp"
#include "dvector.hpp"
using namespace upcxx;


Graph::Graph() {
}

void Graph::populate(const char *path) {
    ifstream fin(path);
    int n, m;
    fin >> n >> m;

    int offset, edge; 
    for (int i = 0; i < n; i++) {
        fin >> offset; 
        offsets.push_back(offset);
    }

    for (int i = 0; i < m; i++) {
        fin >> edge; // edge from current_node to node edge
        edges.push_back(edge);
    }
}

int Graph::out_degree(int n) {
    assert(n<num_nodes());
    if (n == (num_nodes()-1)) {        
        return num_edges()-offsets.get(n).wait();
    }
    
    return offsets.get(n+1).wait() - offsets.get(n).wait();
}

vector<int> Graph::out_neighbors(int n) {
    assert(n<num_nodes());

    auto begin = offsets.get(n).wait();
    auto out_degree = this->out_degree(n);
    vector<int> neighbors(out_degree);
    upcxx::future<> f = make_future();
    for (int i = begin; i < out_degree; i++) {
        f = when_all(f, edges.get(i).then([&neighbors, i, begin](int edge) {neighbors[i-begin] = edge;}));
    }
    f.wait();
    return neighbors;
}

vector<int> Graph::in_neighbors(int n) {
    assert(n<num_nodes());

    vector<int> nodes; 
    int current_node = 0;
    int next_offset = (current_node == (num_nodes()-1)) ? num_nodes() : offsets.get(current_node+1).wait();
    for (int i = 0; i < num_edges(); i++) { 
        // edges from current_node to this->edges[i]
        while (i == next_offset) {
            current_node += 1;
            next_offset = (current_node == (num_nodes()-1)) ? num_edges() : offsets.get(current_node+1).wait();
        }
        if (edges.get(i).wait() == n) {
            nodes.push_back(current_node);
        }
    }

   
    return nodes;
}