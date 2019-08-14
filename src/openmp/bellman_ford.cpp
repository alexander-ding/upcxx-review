#include <iostream>
#include <chrono>
#include <ctime> 
#include <omp.h>


#include <vector>
#include <queue>
#include <climits>
#include <stdlib.h> 
#include <time.h>


#include "graph_weighted.hpp"

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

vector<int> bellman_ford(Graph& g, int root) {
    vector<int> dist(g.num_nodes());
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++)
        dist[i] = INT_MAX; // set INF

    dist[root] = 0;

    int round = 0;
    bool updated_last_round = true;
    // relax procedure
    while (updated_last_round && round < g.num_nodes()) {
        updated_last_round = false;
        #pragma omp parallel for
        for (int u = 0; u < g.num_nodes(); u++) {
            vector<int> neighbors = g.in_neighbors(u);
            vector<int> weights = g.in_weights_neighbors(u);
  
            #pragma omp parallel for if (g.in_degree(u) > THRESHOLD)
            for (int j = 0; j < g.in_degree(u); j++) {
                int v = neighbors[j];
                // if dist is INF, it couldn't update u
                if (dist[v] == INT_MAX) continue;
                int weight = weights[j];
                bool updated_last_round_local = false;

                if (priority_update(&(*dist.begin())+u, dist[v]+weight)) {
                    updated_last_round_local = true;
                }

                compare_and_compare_and_swap(&updated_last_round, updated_last_round, updated_last_round_local);
            }
        }
        round += 1;
    }

    if (round == (g.num_nodes())) {
        // cout << "There exists a negative-weight cycle" << endl;
    }

    return dist;
}

bool compare(int v1, int w, int v2) {
    // checks if v1 + w < v2
    if (v1 == INT_MAX) return false;
    return ((v1+w) < v2);
}

bool verify(Graph& g, int root, vector<int>& input_dist) {
    vector<int> dist(g.num_nodes());
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++)
        dist[i] = INT_MAX; // set INF

    dist[root] = 0;

    int round = 0;
    bool updated_last_round = true;
    // relax procedure
    while (updated_last_round && round < g.num_nodes()) {
        updated_last_round = false;
        #pragma omp parallel for
        for (int u = 0; u < g.num_nodes(); u++) {
            vector<int> neighbors = g.out_neighbors(u);
            vector<int> weights = g.out_weights_neighbors(u);
            #pragma omp parallel for 
            for (int j = 0; j < g.out_degree(u); j++) {
                int v = neighbors[j];
                int weight = weights[j];

                #pragma omp critical
                {
                if (compare(dist[u], weight, dist[v])) { 
                    dist[v] = dist[u] + weight;
                    updated_last_round = updated_last_round || true;
                }
                }
            }
        }
        round += 1;
    }

    if (round == (g.num_nodes())) {
        // cout << "There exists a negative-weight cycle" << endl;
    }

    for (int i = 0; i < input_dist.size(); i++) {
        if (input_dist[i] != dist[i]) {
            return false;
        }
    }
    return true;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Usage: ./bellman_ford <path_to_graph> <num_iters>" << endl;
        exit(-1);
    }
    const int num_iters = atoi(argv[2]);
    
    Graph g(argv[1]);

    float current_time = 0.0;
    srand(time(NULL));
    for (int i = 0; i < num_iters; i++) {
        int root = rand() % g.num_nodes();
        auto time_before = chrono::system_clock::now();
        vector<int> dists = bellman_ford(g, root);

        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
    }
   
    cout << current_time / num_iters << endl;
    
    /*if (!verify(g, root, dists)) {
        cerr << "Wrong!" << endl;
    }*/
}