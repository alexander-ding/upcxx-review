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

#include "graph.hpp"
#include "sequence.hpp"
#include "utils.hpp"

using namespace std;

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
        if (compare_and_swap(addr, old_val, new_val)) {
            return true;
        } else {
            old_val = *addr;
        }
    }
    return false;
}

struct nonNegF{bool operator() (int a) {return (a>=0);}};

int bf_sparse(Graph& g, int* dist, int* dist_next, int* frontier, int* frontier_next, int frontier_size, int level) {
    // update dist_next to take dist's values
    // also reset visited 
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++) {
        dist_next[i] = dist[i];
        frontier_next[i] = -1;
    }

    # pragma omp parallel for
    for (int i = 0; i < frontier_size; i++) {
        int u = frontier[i];
        vector<int> neighbors = g.out_neighbors(u);
        vector<int> weights = g.out_weights_neighbors(u);
        for (int j = 0; j < g.out_degree(u); j++) {
            int v = neighbors[j];
            int relax_dist = dist[u] + weights[j];
            if (priority_update(&dist_next[v], relax_dist)) {
                frontier_next[v] = v;
            }
        }
    }

    frontier_size = sequence::filter(frontier_next, frontier, g.num_nodes(), nonNegF());
    return frontier_size;
}

int bf_dense(Graph& g, int* dist, int* dist_next, bool* frontier, bool* frontier_next, int level) {
    # pragma omp parallel for
    for (int u = 0; u < g.num_nodes(); u++) {
        // update next round of dist
        dist_next[u] = dist[u];
        frontier_next[u] = false;

        vector<int> neighbors = g.in_neighbors(u);
        vector<int> weights = g.in_weights_neighbors(u); 

        for (int j = 0; j < g.in_degree(u); j++) {
            int v = neighbors[j];

            // ignore neighbors not in frontier
            if (!frontier[v]) continue;
            
            int relax_dist = dist[v]+weights[j];
            if (relax_dist < dist_next[u]) {
                dist_next[u] = relax_dist;
                frontier_next[u] = true;
            }
        }
    }

    int frontier_size = sequence::sumFlagsSerial(frontier_next, g.num_nodes());
    return frontier_size;
}

void sparse_to_dense(int* frontier_sparse, int frontier_size, bool* frontier_dense) {
    # pragma omp parallel for
    for (int i = 0; i < frontier_size; i++) {
        frontier_dense[frontier_sparse[i]] = true;
    }
}

void dense_to_sparse(bool* frontier_dense, int num_nodes, int* frontier_sparse) {
    # pragma omp parallel for
    for (int i = 0; i < num_nodes; i++) {
        if (frontier_dense[i]) {
            frontier_sparse[i] = i;
        } else {
            frontier_sparse[i] = -1;
        }
    }
    sequence::filter(frontier_sparse, frontier_sparse, num_nodes, nonNegF());
}

int* bellman_ford(Graph& g, int root) {
    int* dist = newA(int, g.num_nodes());
    int* dist_next = newA(int, g.num_nodes());

    int* frontier_sparse = newA(int, g.num_edges());
    int* frontier_sparse_next = newA(int, g.num_edges());
    bool* frontier_dense = newA(bool, g.num_edges());
    bool* frontier_dense_next = newA(bool, g.num_edges());

    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++) {
        dist[i] = INT_MAX; // set INF
    }

    bool is_sparse_mode = true;

    frontier_sparse[0] = root;
    int frontier_size = 1;
    dist[root] = 0;

    int level = 0;
    const int threshold_fraction_denom = 20; 

    while (frontier_size != 0 && level < g.num_nodes()) {
        level++; 
        bool should_be_sparse_mode = frontier_size < (g.num_nodes() / threshold_fraction_denom);

        cout << "Round " << level << " | " << "Frontier: " << frontier_size << " | Sparse? " << should_be_sparse_mode << endl;
        auto time_before = chrono::system_clock::now();

        if (should_be_sparse_mode) {
            if (!is_sparse_mode) {
                dense_to_sparse(frontier_dense, g.num_nodes(), frontier_sparse);
            }
            is_sparse_mode = true;
            frontier_size = bf_sparse(g, dist, dist_next, frontier_sparse, frontier_sparse_next, frontier_size, level);
        } else {
            if (is_sparse_mode) {
                sparse_to_dense(frontier_sparse, frontier_size, frontier_dense);
            }
            is_sparse_mode = false;
            frontier_size = bf_dense(g, dist, dist_next, frontier_dense, frontier_dense_next, level);
        }
        swap(dist, dist_next);

        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta = (time_after - time_before);
        cout << "Time: " << delta.count() << endl;
    }

    free(dist_next); free(frontier_dense); free(frontier_dense_next); free(frontier_sparse); free(frontier_sparse_next);
    
    return dist;
}


bool compare(int v1, int w, int v2) {
    // checks if v1 + w < v2
    if (v1 == INT_MAX) return false;
    return ((v1+w) < v2);
}

bool verify(Graph& g, int root, int* input_dist) {
    vector<int> dist(g.num_nodes());
    vector<int> dist_next(g.num_nodes());
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++) {
        dist[i] = INT_MAX; // set INF
        dist_next[i] = INT_MAX;
    }

    dist[root] = 0;
    dist_next[root] = 0;

    int round = 0;
    bool updated_last_round = true;
    // relax procedure
    while (updated_last_round && round < g.num_nodes()) {

        updated_last_round = false;
        for (int u = 0; u < g.num_nodes(); u++) {
            vector<int> neighbors = g.out_neighbors(u);
            vector<int> weights = g.out_weights_neighbors(u);
            for (int j = 0; j < g.out_degree(u); j++) {
                int v = neighbors[j];
                int weight = weights[j];

                if (compare(dist[u], weight, dist_next[v])) { 
                    dist_next[v] = dist[u] + weight;
                    updated_last_round = true; 
                }
            }
        }
        for (int i = 0; i < g.num_nodes(); i++)
            dist[i] = dist_next[i];
        round += 1;
    }

    if (round == (g.num_nodes())) {
        // cout << "There exists a negative-weight cycle" << endl;
    }

    for (int i = 0; i < dist.size(); i++) {
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
        int* dists = bellman_ford(g, root);
        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
        /*if (!verify(g, root, dists)) {
            cerr << "Wrong!" << endl;
        }*/
    }
   
    cout << current_time / num_iters << endl;
    
}