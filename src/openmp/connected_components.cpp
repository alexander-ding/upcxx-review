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

struct nonNegF{bool operator() (VertexId a) {return (a>=0);}};
struct trueF{bool operator() (bool a) {return a;}};

VertexId cc_sparse(Graph& g, VertexId* labels, VertexId* labels_next, VertexId* frontier, VertexId* frontier_next, VertexId frontier_size, int level) {
    // update labels_next to take labels's values
    // also reset visited 
    # pragma omp parallel for
    for (VertexId i = 0; i < g.num_nodes; i++) {
        labels_next[i] = labels[i];
        frontier_next[i] = -1;
    }

    # pragma omp parallel for
    for (VertexId i = 0; i < frontier_size; i++) {
        VertexId u = frontier[i];
        VertexId* neighbors = g.out_neighbors(u);
        for (EdgeId j = 0; j < g.out_degree(u); j++) {
            VertexId v = neighbors[j];
            if (priority_update(&labels_next[v], labels[u])) {
                frontier_next[v] = v;
            }
        }

        neighbors = g.in_neighbors(u);
        for (EdgeId j = 0; j < g.in_degree(u); j++) {
            VertexId v = neighbors[j];
            if (priority_update(&labels_next[v], labels[u])) {
                frontier_next[v] = v;
            }
        }        
    }

    frontier_size = sequence::filter(frontier_next, frontier, g.num_nodes, nonNegF());
    return frontier_size;
}

int cc_dense(Graph& g, VertexId* labels, VertexId* labels_next, bool* frontier, bool* frontier_next, int level) {
    # pragma omp parallel for
    for (VertexId i = 0; i < g.num_nodes; i++) {
        frontier_next[i] = false;
        labels_next[i] = labels[i];
    }

    # pragma omp parallel for
    for (VertexId u = 0; u < g.num_nodes; u++) {
        VertexId* neighbors = g.out_neighbors(u);
        for (EdgeId j = 0; j < g.out_degree(u); j++) {
            VertexId v = neighbors[j];
            if (!frontier[v]) continue; // ignore non-frontiers
            if (labels_next[u] > labels[v]) {
                labels_next[u] = labels[v];
                if (!frontier_next[u]) frontier_next[u] = true;
            }
        }

        neighbors = g.in_neighbors(u);
        for (EdgeId j = 0; j < g.in_degree(u); j++) {
            VertexId v = neighbors[j];
            if (!frontier[v]) continue; // ignore non-frontiers
            if (labels_next[u] > labels[v]) {
                labels_next[u] = labels[v];
                if (!frontier_next[u]) frontier_next[u] = true;
            }
        }
    }

    VertexId frontier_size = sequence::sumFlagsSerial(frontier_next, g.num_nodes);
    return frontier_size;
}

void sparse_to_dense(VertexId* frontier_sparse, VertexId frontier_size, bool* frontier_dense) {
    # pragma omp parallel for
    for (VertexId i = 0; i < frontier_size; i++) {
        frontier_dense[frontier_sparse[i]] = true;
    }
}

void dense_to_sparse(bool* frontier_dense, VertexId num_nodes, VertexId* frontier_sparse) {
    # pragma omp parallel for
    for (VertexId i = 0; i < num_nodes; i++) {
        if (frontier_dense[i]) {
            frontier_sparse[i] = i;
        } else {
            frontier_sparse[i] = -1;
        }
    }
    sequence::filter(frontier_sparse, frontier_sparse, num_nodes, nonNegF());
}

VertexId* cc(Graph& g) {
    VertexId* labels = newA(VertexId, g.num_nodes);
    VertexId* labels_next = newA(VertexId, g.num_nodes);

    VertexId* frontier_sparse = newA(VertexId, g.num_nodes);
    VertexId* frontier_sparse_next = newA(VertexId, g.num_nodes);
    bool* frontier_dense = newA(bool, g.num_nodes);
    bool* frontier_dense_next = newA(bool, g.num_nodes);
    # pragma omp parallel for
    for (VertexId i = 0; i < g.num_nodes; i++) {
        labels[i] = i; // set as own group
        frontier_sparse[i] = i;
    }

    VertexId frontier_size = g.num_nodes;

    bool is_sparse_mode = true;

    int level = 0;
    const int threshold_fraction_denom = 20; 

    while (frontier_size != 0) {
        level++; 
        bool should_be_sparse_mode = frontier_size < (g.num_nodes / threshold_fraction_denom);
        if (DEBUG) cout << "Round " << level << " | " << "Frontier: " << frontier_size << " | Sparse? " << should_be_sparse_mode << endl;
        auto time_before = chrono::system_clock::now();
        
        if (should_be_sparse_mode) {
            if (!is_sparse_mode) {
                dense_to_sparse(frontier_dense, g.num_nodes, frontier_sparse);
            }
            is_sparse_mode = true;
            frontier_size = cc_sparse(g, labels, labels_next, frontier_sparse, frontier_sparse_next, frontier_size, level);
        } else {
            if (is_sparse_mode) {
                sparse_to_dense(frontier_sparse, frontier_size, frontier_dense);
            }
            is_sparse_mode = false;
            frontier_size = cc_dense(g, labels, labels_next, frontier_dense, frontier_dense_next, level);
            swap(frontier_dense, frontier_dense_next);
        }
        swap(labels_next, labels); 

        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta = (time_after - time_before);
        if (DEBUG) cout << "Time: " << delta.count() << endl;
        
    }
    free(labels_next); free(frontier_sparse); free(frontier_sparse_next); free(frontier_dense); free(frontier_dense_next); 
    return labels;
}

bool verify(Graph& g, VertexId* labels_new) {
    VertexId* labels = newA(VertexId, g.num_nodes);
    VertexId* labels_next = newA(VertexId, g.num_nodes);
    # pragma omp parallel for
    for (VertexId i = 0; i < g.num_nodes; i++) {
        labels[i] = i;
        labels_next[i] = i;
    }
        

    vector<bool> updated_last_round(g.num_nodes, true);
    vector<bool> updated_last_round_old(g.num_nodes);
    bool updated_last_round_reduced = true;
    // propagate
    do {
        labels = labels_next;
        updated_last_round_reduced = false;
        #pragma omp parallel for
        for (VertexId i = 0; i < g.num_nodes; i++) {
            updated_last_round_old[i] = updated_last_round[i];
            updated_last_round[i] = false;
        }

        #pragma omp parallel for
        for (VertexId u = 0; u < g.num_nodes; u++) {         

            VertexId* neighbors = g.in_neighbors(u);
            for (EdgeId j = 0; j < g.in_degree(u); j++) {
                VertexId v = neighbors[j];
                if (labels_next[u] > labels[v]) {
                    labels_next[u] = labels[v]; 
                    updated_last_round[u] = true;
                }
            }
            neighbors = g.out_neighbors(u);
            for (EdgeId j = 0; j < g.out_degree(u); j++) {
                VertexId v = neighbors[j];
                if (labels_next[u] > labels[v]) {
                    labels_next[u] = labels[v]; 
                    updated_last_round[u] = true;
                }
            }
        }

        #pragma omp parallel for reduction(|| : updated_last_round_reduced)
        for (VertexId i = 0; i < g.num_nodes; i++) {
            updated_last_round_reduced = updated_last_round_reduced || updated_last_round[i];
        }
    } while (updated_last_round_reduced);
    labels = labels_next; 
    for (VertexId i = 0; i < g.num_nodes; i++) {
        // cout << labels[i] << " " << labels_new[i] << endl;
        if (labels[i] != labels_new[i]) {
            return false;
        }
    }

    return true;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Usage: ./connected_components <path_to_graph> <num_iters>" << endl;
        exit(-1);
    }
    
    Graph g(argv[1]);
    int num_iters = atoi(argv[2]);
    float current_time = 0.0;
    for (int i = 0; i < num_iters; i++) {
        auto time_before = chrono::system_clock::now();
        VertexId* labels = cc(g);
        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
        /* if (!verify(g, labels)) {
            cerr << "Wrong!" << endl;
        } */
    }

    
    cout << current_time / num_iters << endl;

    
}