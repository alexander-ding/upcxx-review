#include <iostream>
#include <chrono>
#include <ctime> 
#include <omp.h>

#include <vector>
#include <queue>
#include <climits>

#include "graph.hpp"
#include "sequence.hpp"
#include "utils.hpp"

using namespace std;

double scan_in(Graph& g) {
    auto time_before = chrono::system_clock::now();
    # pragma omp parallel for
    for (VertexId u = 0; u < g.num_nodes; u++) {
        VertexId* neighbors = g.in_neighbors(u);
        for (EdgeId j = 0; j < g.in_degree(u); j++) {
            VertexId v = neighbors[j];
        }
    }
    auto time_after = chrono::system_clock::now();
    chrono::duration<double> delta = (time_after - time_before);
    return delta.count();
}

double scan_out(Graph& g) {
    auto time_before = chrono::system_clock::now();
    # pragma omp parallel for
    for (VertexId u = 0; u < g.num_nodes; u++) {
        VertexId* neighbors = g.out_neighbors(u);
        for (EdgeId j = 0; j < g.out_degree(u); j++) {
            VertexId v = neighbors[j];
        }
    }
    auto time_after = chrono::system_clock::now();
    chrono::duration<double> delta = (time_after - time_before);
    return delta.count();
}

bool check_graph(Graph& g) {
    for (VertexId u = 0; u < g.num_nodes; u++) {
        VertexId* neighbors = g.out_neighbors(u);
        for (EdgeId j = 0; j < g.out_degree(u); j++) {
            bool exists_in_neighbors = false;
            VertexId v = neighbors[j];
            VertexId* in_neighbors = g.in_neighbors(v);
            for (EdgeId k = 0; k < g.in_degree(v); k++) {
                if (in_neighbors[k] == u) {
                    exists_in_neighbors = true;
                    break;
                }
            }
            if (!exists_in_neighbors) {
                return false;
            }
        }
    }
    for (VertexId u = 0; u < g.num_nodes; u++) {
        VertexId* neighbors = g.in_neighbors(u);
        for (EdgeId j = 0; j < g.in_degree(u); j++) {
            bool exists_out_neighbors = false;
            VertexId v = neighbors[j];
            VertexId* out_neighbors = g.out_neighbors(v);
            for (EdgeId k = 0; k < g.out_degree(v); k++) {
                if (out_neighbors[k] == u) {
                    exists_out_neighbors = true;
                    break;
                }
            }
            if (!exists_out_neighbors) {
                return false;
            }
        }
    }
    return true;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cout << "Usage: ./graph_scan <path_to_graph>" << endl;
        exit(-1);
    }
    
    Graph g(argv[1]);

    auto time_in = scan_in(g);
    cout << "Scan in took " << time_in << endl;
    auto time_out = scan_out(g);
    cout << "Scan out took " << time_out << endl;

    if (check_graph(g)) {
        cout << "Graph check success" << endl;
    }
}
