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


bool priority_update(int* addr, int new_val) {
    int old_val = *addr;
    while (new_val < old_val) {
        if (__sync_bool_compare_and_swap(addr, old_val, new_val)) {
            return true;
        } else {
            old_val = *addr;
        }
    }
    return false;
}

struct nonNegF{bool operator() (int a) {return (a>=0);}};

int cc_sparse(Graph& g, int* labels, int* labels_next, int* frontier, int* frontier_next, int frontier_size, int level) {
    // update labels_next to take labels's values
    // also reset visited 
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++) {
        labels_next[i] = labels[i];
        frontier_next[i] = -1;
    }
    # pragma omp parallel for
    for (int i = 0; i < frontier_size; i++) {
        int u = frontier[i];
        vector<int> neighbors = g.out_neighbors(u);
        for (int j = 0; j < g.out_degree(u); j++) {
            int v = neighbors[j];
            if (priority_update(&labels_next[v], labels[u])) {
                frontier_next[v] = v;
            }
        }

        neighbors = g.in_neighbors(u);
        for (int j = 0; j < g.in_degree(u); j++) {
            int v = neighbors[j];
            if (priority_update(&labels_next[v], labels[u])) {
                frontier_next[v] = v;
            }
        }        
    }
    

    frontier_size = sequence::filter(frontier_next, frontier, g.num_nodes(), nonNegF());
    return frontier_size;
}

int cc_dense(Graph& g, int* labels, int* labels_next, int* frontier, int level) {
    # pragma omp parallel for
    for (int u = 0; u < g.num_nodes(); u++) {
        // update next round of labels
        labels_next[u] = labels[u];

        vector<int> neighbors = g.out_neighbors(u);
        for (int j = 0; j < g.out_degree(u); j++) {
            int v = neighbors[j];
            // no worry for contention, as every vertex
            // is processed by one thread at a time
            if (labels[v] < labels_next[u]) {
                labels_next[u] = labels[v];
            }
        }

        neighbors = g.in_neighbors(u);
        for (int j = 0; j < g.in_degree(u); j++) {
            int v = neighbors[j];
            if (labels[v] < labels_next[u]) {
                labels_next[u] = labels[v];
            }
        }
    }

    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++) {
        if (labels_next[i] != labels[i]) {
            frontier[i] = i;
        } else {
            frontier[i] = -1;
        }
    }
    int frontier_size = sequence::filter(frontier, frontier, g.num_nodes(), nonNegF());
    return frontier_size;
}

int* cc(Graph& g) {
    int* labels = newA(int, g.num_nodes());
    int* labels_next = newA(int, g.num_nodes());

    int* frontier = newA(int, g.num_nodes());
    int* frontier_next = newA(int, g.num_nodes());
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++) {
        labels[i] = i; // set as own group
        frontier[i] = i;
    }

    int frontier_size = g.num_nodes();

    bool is_sparse_mode = true;

    int level = 0;
    const int threshold_fraction_denom = 20; 

    while (frontier_size != 0) {
        level++; 
        bool should_be_sparse_mode = frontier_size < (g.num_nodes() / threshold_fraction_denom);
        cout << "Round " << level << " | " << "Frontier: " << frontier_size << " | Sparse? " << should_be_sparse_mode << endl;
        auto time_before = chrono::system_clock::now();
        
        if (should_be_sparse_mode) {
            frontier_size = cc_sparse(g, labels, labels_next, frontier, frontier_next, frontier_size, level);
        } else {
            frontier_size = cc_dense(g, labels, labels_next, frontier, level);
        }
        swap(labels_next, labels); 

        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta = (time_after - time_before);
        cout << "Time: " << delta.count() << endl;
    }
    free(labels_next); free(frontier); free(frontier_next);
    return labels;
}

bool verify(Graph& g, int* labels_new) {
    vector<int> labels(g.num_nodes());
    vector<int> labels_next(g.num_nodes());
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++) {
        labels[i] = i;
        labels_next[i] = i;
    }
        

    vector<bool> updated_last_round(g.num_nodes(), true);
    vector<bool> updated_last_round_old(g.num_nodes());
    bool updated_last_round_reduced = true;
    // propagate
    do {
        labels = labels_next;
        updated_last_round_reduced = false;
        #pragma omp parallel for
        for (int i = 0; i < g.num_nodes(); i++) {
            updated_last_round_old[i] = updated_last_round[i];
            updated_last_round[i] = false;
        }

        #pragma omp parallel for
        for (int u = 0; u < g.num_nodes(); u++) {         

            vector<int> neighbors = g.in_neighbors(u);
            #pragma omp parallel for
            for (int j = 0; j < g.in_degree(u); j++) {
                int v = neighbors[j];
                #pragma omp critical 
                {
                if (labels_next[u] > labels[v]) {
                    labels_next[u] = labels[v]; 
                    updated_last_round[u] = true;
                } 
                }
            }
            neighbors = g.out_neighbors(u);
            #pragma omp parallel for
            for (int j = 0; j < g.out_degree(u); j++) {
                int v = neighbors[j];
                #pragma omp critical 
                {
                if (labels_next[u] > labels[v]) {
                    labels_next[u] = labels[v]; 
                    updated_last_round[u] = true;
                } 
                }
            }
        }

        #pragma omp parallel for reduction(|| : updated_last_round_reduced)
        for (int i = 0; i < g.num_nodes(); i++) {
            updated_last_round_reduced = updated_last_round_reduced || updated_last_round[i];
        }
    } while (updated_last_round_reduced);
    labels = labels_next; 
    for (int i = 0; i < labels.size(); i++) {
        cout << labels[i] << " " << labels_new[i] << endl;
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
        int* labels = cc(g);
        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
        if (!verify(g, labels)) {
            cerr << "Wrong!" << endl;
        }
    }

    
    cout << current_time / num_iters << endl;

    
}