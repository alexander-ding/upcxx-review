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

int bf_sparse(Graph& g, int* dist, int* dist_next, int* counts, int* frontier, int* frontier_next, int frontier_size, bool* visited, int level) {
    // compute the counts for the newly generated frontier
    # pragma omp parallel for
    for (int i = 0; i < frontier_size; i++)
        counts[i] = g.out_degree(frontier[i]);

    int counts_size = sequence::plusScan(counts, counts, frontier_size);

    // update dist_next to take dist's values
    // also reset visited 
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++) {
        visited[i] = false;
        dist_next[i] = dist[i];
    }

    # pragma omp parallel for
    for (int i = 0; i < frontier_size; i++) {
        int u = frontier[i];
        int count = counts[i];
        vector<int> neighbors = g.out_neighbors(u);
        vector<int> weights = g.out_weights_neighbors(u);
        for (int j = 0; j < g.out_degree(u); j++) {
            int v = neighbors[j];
            int relax_dist = dist[u] + weights[j];
            if (priority_update(&dist_next[v], relax_dist) && !visited[v]) {
                frontier_next[count+j] = v;
                visited[v] = true;
            } else {
                frontier_next[count+j] = -1;
            }
        }
    }

    frontier_size = sequence::filter(frontier_next, frontier, counts_size, nonNegF());
    return frontier_size;
}

int bf_dense(Graph& g, int* dist, int* dist_next, int* counts, int level) {
    # pragma omp parallel for
    for (int u = 0; u < g.num_nodes(); u++) {
        // update next round of dist
        dist_next[u] = dist[u];

        vector<int> neighbors = g.in_neighbors(u);
        vector<int> weights = g.in_weights_neighbors(u); 

        for (int j = 0; j < g.in_degree(u); j++) {
            int v = neighbors[j];

            // if dist is INF, it can't update u
            if (dist[v] == INT_MAX) continue;
            
            int relax_dist = dist[v]+weights[j];
    
            priority_update(&dist_next[u], relax_dist);
        }
    }

    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++)
        counts[i] = 0;
    
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++)
        if (dist_next[i] != dist[i])
            counts[i] = 1;

    int frontier_size = sequence::plusScan(counts, counts, g.num_nodes());
    
    return frontier_size;
}

int* bellman_ford(Graph& g, int root) {
    int* dist = newA(int, g.num_nodes());
    int* dist_next = newA(int, g.num_nodes());
    // for each vertex in the frontier, the number of neighbors it has
    int* counts = newA(int, g.num_nodes());
    int* frontier = newA(int, g.num_edges());
    int* frontier_next = newA(int, g.num_edges());
    bool* visited = newA(bool, g.num_nodes());
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++) {
        dist[i] = INT_MAX; // set INF
    }

    bool is_sparse_mode = true;

    frontier[0] = root;
    int frontier_size = 1;
    dist[root] = 0;

    int level = 0;
    const int threshold_fraction_denom = 20; 

    while (frontier_size != 0 && level < g.num_nodes()) {
        level++; 
        bool should_be_sparse_mode = frontier_size < (g.num_nodes() / threshold_fraction_denom);
        if (should_be_sparse_mode) {
            if (!is_sparse_mode) {
                // clear counts
                # pragma omp parallel for
                for (int i = 0; i < g.num_nodes(); i++)
                    counts[i] = 0;
                
                // set counts to 1 where it is frontier
                // i.e., where dist has changed in last round
                # pragma omp parallel for
                for (int i = 0; i < g.num_nodes(); i++)
                    if (dist_next[i] != dist[i])
                        counts[i] = 1;
                
                // take prefix sum for counts
                sequence::plusScan(counts, counts, g.num_nodes());

                // set the frontier
                # pragma omp parallel for
                for (int i = 0; i < g.num_nodes() - 1; i++)
                    if (counts[i] != counts[i+1])
                        frontier[counts[i]] = i;

                // handle the special case of the last member
                if (counts[g.num_nodes()-1] == frontier_size-1)
                    frontier[frontier_size-1] = g.num_nodes()-1;
            }

            is_sparse_mode = true;
            frontier_size = bf_sparse(g, dist, dist_next, counts, frontier, frontier_next, frontier_size, visited, level);
        } else {
            // dense version
            is_sparse_mode = false;
            frontier_size = bf_dense(g, dist, dist_next, counts, level);
        }
        swap(dist, dist_next);
    }

    free(dist_next); free(frontier); free(frontier_next); free(counts); free(visited);
    
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