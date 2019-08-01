#include <iostream>
#include <chrono>
#include <ctime> 
#include <omp.h>

#include <vector>
#include <queue>
#include <climits>

#include "graph.hpp"

using namespace std;

template <typename T>
void print_vector(const vector<T> & v) {
    cout << endl;
    for (auto i = v.begin(); i != v.end(); ++i) {
        cout << *i << " ";
    }
    cout << endl;
}

vector<int> connected_components(Graph& g) {
    vector<int> labels(g.num_nodes());
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++)
        labels[i] = i;

    vector<bool> updated_last_round(g.num_nodes(), true);
    vector<bool> updated_last_round_old(g.num_nodes());
    bool updated_last_round_reduced = true;
    // propagate
    while (updated_last_round_reduced) {
        updated_last_round_reduced = false;
        #pragma omp parallel for
        for (int i = 0; i < g.num_nodes(); i++) {
            updated_last_round_old[i] = updated_last_round[i];
            updated_last_round[i] = false;
        }

        #pragma omp parallel for
        for (int u = 0; u < g.num_nodes(); u++) {         
            if (!updated_last_round_old[u]) continue;

            vector<int> neighbors = g.out_neighbors(u);
            #pragma omp parallel for
            for (int j = 0; j < g.out_degree(u); j++) {
                int v = neighbors[j];
                #pragma omp critical 
                {
                if (labels[u] < labels[v]) {
                    labels[v] = labels[u]; 
                    updated_last_round[v] = true;
                }
                }
            }
        }

        #pragma omp parallel for reduction(|| : updated_last_round_reduced)
        for (int i = 0; i < g.num_nodes(); i++) {
            updated_last_round_reduced = updated_last_round_reduced || updated_last_round[i];
        }
    }

    return labels;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cout << "Usage: ./connected_components <path_to_graph>" << endl;
        exit(-1);
    }
    
    Graph g(argv[1]);

    auto time_before = chrono::system_clock::now();
    vector<int> labels = connected_components(g);
    auto time_after = chrono::system_clock::now();
    chrono::duration<double> delta_time = time_after - time_before;
    cout << delta_time.count() << endl;
}