#include <omp.h>

#include <iostream>
#include <chrono>
#include <ctime> 

#include <vector>
#include <queue>
#include <climits>

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

struct nonNegF{bool operator() (VertexId a) {return (a>=0);}};

VertexId bfs_sparse(Graph& g, int* dist, int* dist_next, VertexId* frontier, VertexId* frontier_next, VertexId frontier_size, VertexId level) { 
    // update dist_next to take dist's values
    # pragma omp parallel for
    for (VertexId i = 0; i < g.num_nodes; i++) {
        dist_next[i] = dist[i];
        frontier_next[i] = -1;
    } 

    # pragma omp parallel for
    for (VertexId i = 0; i < frontier_size; i++) {
        
        VertexId u = frontier[i];
        VertexId* neighbors = g.out_neighbors(u);
        for (EdgeId j = 0; j < g.out_degree(u); j++) {
            VertexId v = neighbors[j];
            if (compare_and_swap(&dist_next[v], INT_MAX, level)) {
                frontier_next[v] = v;
            }
        }
    }

    frontier_size = sequence::filter(frontier_next, frontier, g.num_nodes, nonNegF());
    return frontier_size;
}

VertexId bfs_dense(Graph& g, int* dist, int* dist_next, bool* frontier, bool* frontier_next, VertexId level) {
    # pragma omp parallel for
    for (VertexId u = 0; u < g.num_nodes; u++) {
        // update next round of dist
        dist_next[u] = dist[u];
        frontier_next[u] = false;
        // ignore if distance is set already
        if (dist_next[u] != INT_MAX) continue;
        VertexId* neighbors = g.in_neighbors(u);
        for (EdgeId j = 0; j < g.in_degree(u); j++) {
            VertexId v = neighbors[j];

            if (!frontier[v]) continue;
            
            dist_next[u] = level;
            frontier_next[u] = true;
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

int* bfs(Graph& g, VertexId root) {
    int* dist = newA(int, g.num_nodes);
    int* dist_next = newA(int, g.num_nodes);

    VertexId* frontier_sparse = newA(VertexId, g.num_nodes);
    VertexId* frontier_sparse_next = newA(VertexId, g.num_nodes);
    bool* frontier_dense = newA(bool, g.num_nodes);
    bool* frontier_dense_next = newA(bool, g.num_nodes);

    # pragma omp parallel for
    for (VertexId i = 0; i < g.num_nodes; i++) {
        dist[i] = INT_MAX; // set INF
    }

    bool is_sparse_mode = true;

    frontier_sparse[0] = root;
    VertexId frontier_size = 1;
    dist[root] = 0;

    VertexId level = 0;
    const int threshold_fraction_denom = 20; 

    while (frontier_size != 0) {
        level++; 
        bool should_be_sparse_mode = frontier_size < (g.num_nodes / threshold_fraction_denom);

        if (DEBUG) cout << "Round " << level << " | " << "Frontier: " << frontier_size << " | Sparse? " << should_be_sparse_mode << endl;
        auto time_before = chrono::system_clock::now();

        if (should_be_sparse_mode) {
            if (!is_sparse_mode) {
                dense_to_sparse(frontier_dense_next, g.num_nodes, frontier_sparse);
            }
            is_sparse_mode = true;
            frontier_size = bfs_sparse(g, dist, dist_next, frontier_sparse, frontier_sparse_next, frontier_size, level);
        } else {
            if (is_sparse_mode) {
                sparse_to_dense(frontier_sparse, frontier_size, frontier_dense);
            }
            is_sparse_mode = false;
            frontier_size = bfs_dense(g, dist, dist_next, frontier_dense, frontier_dense_next, level);
            swap(frontier_dense, frontier_dense_next);
        }
        swap(dist_next, dist); 

        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta = (time_after - time_before);
        if (DEBUG) cout << "Time: " << delta.count() << endl;
    }
    free(dist_next); free(frontier_dense); free(frontier_dense_next); free(frontier_sparse); free(frontier_sparse_next);
    return dist;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Usage: ./bfs <path_to_graph> <num_iter>" << endl;
        exit(-1);
    }

    int num_iters = atoi(argv[2]);
    
    Graph g(argv[1]);

    float current_time = 0.0;
    srand(time(NULL));
    for (int i = 0; i < num_iters; i++) {
        VertexId root = rand() % g.num_nodes;
        auto time_before = chrono::system_clock::now();
        int* scores = bfs(g, root);
        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
    }
    cout << current_time / num_iters << endl;

    
}