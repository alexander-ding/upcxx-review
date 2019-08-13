#include <iostream>
#include <chrono>
#include <ctime> 
#include <omp.h>

#include <vector>
#include <queue>
#include <climits>

#include "graph.hpp"

using namespace std;

const int THRESHOLD = 1000;

template <typename T>
void print_vector(const vector<T> & v) {
    cout << endl;
    for (auto i = v.begin(); i != v.end(); ++i) {
        cout << *i << " ";
    }
    cout << endl;
}

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

void compare_and_compare_and_swap(bool* addr, bool old_val, bool new_val) {
    // do nothing if already true
    if (old_val) {
        return;
    }
    __sync_bool_compare_and_swap(addr, old_val, new_val);
}

vector<int> connected_components(Graph& g) {
    vector<int> labels(g.num_nodes());
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++)
        labels[i] = i;

    vector<bool> updated_last_round(g.num_nodes(), true);
    vector<bool> updated_last_round_old(g.num_nodes());
    bool any_updated_last_round = true;
    // propagate
    while (any_updated_last_round) {
        any_updated_last_round = false;
        #pragma omp parallel for
        for (int i = 0; i < g.num_nodes(); i++) {
            updated_last_round_old[i] = updated_last_round[i];
            updated_last_round[i] = false;
        }

        #pragma omp parallel for
        for (int u = 0; u < g.num_nodes(); u++) {         
            vector<int> neighbors = g.in_neighbors(u);
            #pragma omp parallel for if (g.in_degree(u) > THRESHOLD)
            for (int j = 0; j < g.in_degree(u); j++) {
                int v = neighbors[j];
                if (!updated_last_round_old[v]) continue;
                if (priority_update(&(*labels.begin())+u, labels[v])) {
                    updated_last_round[u] = true;
                }
                compare_and_compare_and_swap(&any_updated_last_round, any_updated_last_round, updated_last_round[u]);
            }
        }

    }

    return labels;
}

bool verify(Graph& g, vector<int>& labels_new) {
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
    print_vector(labels);
    print_vector(labels_new);
    for (int i = 0; i < labels_new.size(); i++) {
        if (labels[i] != labels_new[i]) {
            return false;
        }
    }

    return true;
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

    /*if (!verify(g, labels)) {
        cerr << "Wrong!" << endl;
    }*/
}