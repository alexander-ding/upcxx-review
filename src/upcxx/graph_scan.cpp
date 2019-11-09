#include <iostream>
#include "upcxx/upcxx.hpp"
#include "graph.hpp"

using namespace std;
using namespace upcxx;

void scan_in(Graph& g) {
    for (VertexId u = g.rank_start; u < g.rank_end; u++) {
        VertexId* neighbors = g.in_neighbors(u).local();
        cout << u << " has in neighbors" << endl;
        for (EdgeId j = 0; j < g.in_degree(u); j++) {
            VertexId v = neighbors[j];
            cout << v << endl;
        }
    }
}

void scan_out(Graph& g) {
    for (VertexId u = g.rank_start; u < g.rank_end; u++) {
        VertexId* neighbors = g.out_neighbors(u).local();
        cout << u << " has out neighbors" << endl;
        for (EdgeId j = 0; j < g.out_degree(u); j++) {
            VertexId v = neighbors[j];
            cout << v << endl;
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cout << "Usage: ./graph_scan <path_to_graph>" << endl;
        exit(-1);
    }

    init();
    Graph g = Graph(argv[1]);
    barrier();
    auto time_before = chrono::system_clock::now();
    scan_in(g);
    barrier();
    auto time_after = chrono::system_clock::now();
    chrono::duration<double> delta = (time_after - time_before);
    if (rank_me() == 0)
        cout << "Scan in took " << delta.count() << endl;
    
    time_before = chrono::system_clock::now();
    scan_out(g);
    barrier();
    time_after = chrono::system_clock::now();
    delta = (time_after - time_before);
    if (rank_me() == 0)
        cout << "Scan out took " << delta.count() << endl;
}