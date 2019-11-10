#include "graph_weighted.hpp"
#include "upcxx/upcxx.hpp"
#include <chrono>
#include <ctime> 
#include <climits>
#include <stdlib.h> 
#include <time.h>
#include "sequence.hpp"

using namespace upcxx;

struct nonNegF{bool operator() (VertexId a) {return (a>=0);}};

void sync_round_sparse(Graph& g, int* dist_next, VertexId* frontier_next) {
    reduce_all(dist_next, dist_next, g.num_nodes, op_fast_min).wait();
    reduce_all(frontier_next, frontier_next, g.num_nodes, op_fast_max).wait();
    barrier();
}

VertexId bf_sparse(Graph& g, global_ptr<int> dist_dist, global_ptr<int> dist_next_dist, global_ptr<VertexId> frontier_dist, global_ptr<VertexId> frontier_next_dist, VertexId frontier_size, VertexId level) {
    int* dist = dist_dist.local();
    int* dist_next = dist_next_dist.local();
    VertexId* frontier = frontier_dist.local();
    VertexId* frontier_next = frontier_next_dist.local();

    for (VertexId i = 0; i < g.num_nodes; i++) {
        dist_next[i] = dist[i];
        frontier_next[i] = -1;
    }

    for (VertexId i = 0; i < frontier_size; i++) {
        VertexId u = frontier[i];
        if (!(g.rank_start <= u && u < g.rank_end)) continue;
        VertexId* neighbors = g.out_neighbors(u).local();
        int* weights = g.out_weights_neighbors(u).local();
        for (EdgeId j = 0; j < g.out_degree(u); j++) {
            VertexId v = neighbors[j];
            int relax_dist = dist[u] + weights[j];
            if (priority_update(&dist_next[v], relax_dist)) {
                frontier_next[v] = v;
            }
        }
    }
    barrier();
    sync_round_sparse(g, dist_next, frontier_next);
    frontier_size = sequence::filter(frontier_next, frontier, g.num_nodes, nonNegF());
    return frontier_size; 
}

void sync_round_dense(Graph& g, int* dist_next, bool* frontier_next) {  
    for (VertexId i = 0; i < rank_n(); i++) {
        broadcast(dist_next+g.rank_start_node(i), g.rank_num_nodes(i), i).wait();
        broadcast(frontier_next+g.rank_start_node(i), g.rank_num_nodes(i), i).wait();
    }
    barrier();
}

VertexId bf_dense(Graph& g, global_ptr<int> dist_dist, global_ptr<int> dist_next_dist, global_ptr<bool> frontier_dist, global_ptr<bool> frontier_next_dist, VertexId level) {
    int* dist = dist_dist.local();
    int* dist_next = dist_next_dist.local();
    bool* frontier = frontier_dist.local();
    bool* frontier_next = frontier_next_dist.local();

    for (VertexId u = 0; u < g.num_nodes; u++) {
        // update next round of dist
        dist_next[u] = dist[u];
        frontier_next[u] = false;
    }

    for (VertexId u = g.rank_start; u < g.rank_end; u++) {
        VertexId* neighbors = g.in_neighbors(u).local(); 
        int* weights = g.in_weights_neighbors(u).local();

        for (EdgeId j = 0; j < g.in_degree(u); j++) {
            VertexId v = neighbors[j];

            if (!frontier[v]) continue;

            int relax_dist = dist[v] + weights[j];
            if (relax_dist < dist_next[u]) {
                dist_next[u] = relax_dist; 
                compare_and_compare_and_swap(&frontier_next[u]);
            }
        }

    }
    barrier();
    sync_round_dense(g, dist_next, frontier_next);
    VertexId frontier_size = sequence::sumFlagsSerial(frontier_next, g.num_nodes);

    return frontier_size;
}


void sparse_to_dense(VertexId* frontier_sparse, VertexId frontier_size, bool* frontier_dense) {
    for (VertexId i = 0; i < frontier_size; i++) {
        frontier_dense[frontier_sparse[i]] = true;
    }
}

void dense_to_sparse(bool* frontier_dense, VertexId num_nodes, VertexId* frontier_sparse) {
    for (VertexId i = 0; i < num_nodes; i++) {
        if (frontier_dense[i]) {
            frontier_sparse[i] = i;
        } else {
            frontier_sparse[i] = -1;
        }
    }
    sequence::filter(frontier_sparse, frontier_sparse, num_nodes, nonNegF());
}

int* bellman_ford(Graph &g, VertexId root) {
    // https://github.com/sbeamer/gapbs/blob/master/src/pr.cc
    global_ptr<int> dist_dist = new_array<int>(g.num_nodes); int* dist = dist_dist.local();
    global_ptr<int> dist_next_dist = new_array<int>(g.num_nodes); int* dist_next = dist_next_dist.local();

    global_ptr<VertexId> frontier_sparse_dist = new_array<VertexId>(g.num_nodes); VertexId* frontier_sparse = frontier_sparse_dist.local();
    global_ptr<VertexId> frontier_sparse_next_dist = new_array<VertexId>(g.num_nodes); VertexId* frontier_sparse_next = frontier_sparse_next_dist.local();

    global_ptr<bool> frontier_dense_dist = new_array<bool>(g.num_nodes); bool* frontier_dense = frontier_dense_dist.local();
    global_ptr<bool> frontier_dense_next_dist = new_array<bool>(g.num_nodes); bool* frontier_dense_next = frontier_dense_next_dist.local();

    for (int i = 0; i < g.num_nodes; i++) {
        dist[i] = INT_MAX;
    }

    bool is_sparse_mode = true;

    frontier_sparse[0] = root;
    VertexId frontier_size = 1;
    dist[root] = 0; // initialize everyone to INF except root

    VertexId level = 0;
    const int threshold_fraction_denom = 20;

    while (frontier_size != 0 && level < g.num_nodes) {
        level++; 
        bool should_be_sparse_mode = frontier_size < (g.num_nodes / threshold_fraction_denom);

        if (DEBUG && rank_me() == 0) cout << "Round " << level << " | " << "Frontier: " << frontier_size << " | Sparse? " << should_be_sparse_mode << endl;
        auto time_before = chrono::system_clock::now();

        if (should_be_sparse_mode) {
            if (!is_sparse_mode) {
                dense_to_sparse(frontier_dense, g.num_nodes, frontier_sparse);
            }
            is_sparse_mode = true;
            frontier_size = bf_sparse(g, dist_dist, dist_next_dist, frontier_sparse_dist, frontier_sparse_next_dist, frontier_size, level);
        } else {
            if (is_sparse_mode) {
                sparse_to_dense(frontier_sparse, frontier_size, frontier_dense);
            }
            is_sparse_mode = false;
            frontier_size = bf_dense(g, dist_dist, dist_next_dist, frontier_dense_dist, frontier_dense_next_dist, level);

            swap(frontier_dense_next_dist, frontier_dense_dist);
            swap(frontier_dense_next, frontier_dense);
        }

        swap(dist_next_dist, dist_dist);
        swap(dist_next, dist);
        barrier();
        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta = (time_after - time_before);
        if (DEBUG && rank_me() == 0) cout << "Time: " << delta.count() << endl;
    }

    delete_array(dist_next_dist); delete_array(frontier_sparse_dist); delete_array(frontier_sparse_next_dist); delete_array(frontier_dense_dist); delete_array(frontier_dense_next_dist);

    return dist; 
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Usage: ./bellman_ford <path_to_graph> <num_iters>" << endl;
        exit(-1);
    }
    
    init();

    Graph g(argv[1]);
    int num_iters = atoi(argv[2]);

    barrier(); 
    srand(time(NULL));
    float current_time = 0.0;
    for (int i = 0; i < num_iters; i++) {
        int root = rand() % g.num_nodes;
        root = broadcast(root, 0).wait();
        auto time_before = std::chrono::system_clock::now();
        int* dist = bellman_ford(g, root);
        auto time_after = std::chrono::system_clock::now();
        std::chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
        /* if (rank_me() == 0) {
            for (int i = 0; i < g.num_nodes; i++)
                cout << dist[i] << endl;
        } */
        barrier();
    }
    
    if (rank_me() == 0) {
        std::cout << current_time / num_iters << std::endl;
        /* if (!verify(g, root, dist)) {
            std::cerr << "Verification not correct" << endl;
        } */
    }
    barrier();
    finalize();
}