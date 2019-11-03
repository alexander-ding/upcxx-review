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

struct nonNegF{bool operator() (int a) {return (a>=0);}};

int bfs_sparse(Graph& g, int* dist, int* counts, int* frontier, int* frontier_next, int frontier_size, int level) {
    // compute the counts for the newly generated frontier
    # pragma omp parallel for
    for (int i = 0; i < frontier_size; i++)
        counts[i] = g.out_degree(frontier[i]);

    int counts_size = sequence::plusScan(counts, counts, frontier_size);
    
    # pragma omp parallel for
    for (int i = 0; i < frontier_size; i++) {
        int u = frontier[i];
        int count = counts[i];
        vector<int> neighbors = g.out_neighbors(u);
        for (int j = 0; j < g.out_degree(u); j++) {
            int v = neighbors[j];
            if (dist[v] == INT_MAX && compare_and_swap(&dist[v], INT_MAX, level)) {
                frontier_next[count+j] = v;
            } else {
                frontier_next[count+j] = -1;
            }
        }
    }

    frontier_size = sequence::filter(frontier_next, frontier, counts_size, nonNegF());
    return frontier_size;
}

int bfs_dense(Graph& g, int* dist, int* dist_next, int* counts, int level) {
    # pragma omp parallel for
    for (int u = 0; u < g.num_nodes(); u++) {
        // update next round of dist
        dist_next[u] = dist[u];
        // ignore if distance is set already
        if (dist[u] != INT_MAX) continue;
        vector<int> neighbors = g.in_neighbors(u);
        for (int j = 0; j < g.in_degree(u); j++) {
            int v = neighbors[j];
            if (dist[v] == (level-1)) {
                dist_next[u] = level;
                break;
            }
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

int* bfs(Graph& g, int root) {
    int* dist = newA(int, g.num_nodes());
    int* dist_next = newA(int, g.num_nodes());
    // for each vertex in the frontier, the number of neighbors it has
    int* counts = newA(int, g.num_nodes());
    int* frontier = newA(int, g.num_edges());
    int* frontier_next = newA(int, g.num_edges());
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++) {
        dist[i] = INT_MAX; // set INF
        dist_next[i] = INT_MAX;
    }

    bool is_sparse_mode = true;

    frontier[0] = root;
    int frontier_size = 1;
    dist[root] = 0;

    int level = 0;
    const int threshold_fraction_denom = 20; 

    while (frontier_size != 0) {
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
            frontier_size = bfs_sparse(g, dist, counts, frontier, frontier_next, frontier_size, level);
        } else {
            // dense version
            is_sparse_mode = false;
            frontier_size = bfs_dense(g, dist, dist_next, counts, level);
            swap(dist_next, dist); 
        }
    }
    free(dist_next); free(frontier); free(frontier_next); free(counts);
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
        int root = rand() % g.num_nodes();
        auto time_before = chrono::system_clock::now();
        int* scores = bfs(g, root);
        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
    }
    cout << current_time / num_iters << endl;

    /* if (!verify(g, root, scores)) {
        cerr << "BFS result is not correct" << endl;
    } */
}