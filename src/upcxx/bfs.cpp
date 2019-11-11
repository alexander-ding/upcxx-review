#include "graph.hpp"
#include "upcxx/upcxx.hpp"
#include <chrono>
#include <ctime> 
#include <climits>
#include <stdlib.h> 
#include <time.h>
#include "sequence.hpp"
#include <omp.h>

using namespace upcxx;
typedef VertexId Distance;

struct nonNegF{bool operator() (VertexId a) {return (a>=0);}};

void sync_round_sparse(Graph& g, Distance* dist_next, VertexId* frontier_next) {
    promise<> p;
    reduce_all(dist_next, dist_next, g.num_nodes, op_fast_min, world(), operation_cx::as_promise(p));
    reduce_all(frontier_next, frontier_next, g.num_nodes, op_fast_max, world(), operation_cx::as_promise(p));
    p.finalize().wait();
    barrier();
}

VertexId bfs_sparse(Graph& g, global_ptr<Distance> dist_dist, global_ptr<Distance> dist_next_dist, global_ptr<VertexId> frontier_dist, global_ptr<VertexId> frontier_next_dist, VertexId frontier_size, VertexId level) {
    auto time_1 = chrono::system_clock::now();
    Distance* dist = dist_dist.local();
    Distance* dist_next = dist_next_dist.local();
    VertexId* frontier = frontier_dist.local();
    VertexId* frontier_next = frontier_next_dist.local();
    
    # pragma omp parallel for
    for (VertexId i = 0; i < g.num_nodes; i++) {
        dist_next[i] = dist[i];
        frontier_next[i] = -1;
    }

    # pragma omp parallel for
    for (VertexId i = 0; i < frontier_size; i++) {
        VertexId u = frontier[i];
        if (!(g.rank_start <= u && u < g.rank_end)) continue;
        VertexId* neighbors = g.out_neighbors(u).local();
        for (EdgeId j = 0; j < g.out_degree(u); j++) {
            VertexId v = neighbors[j];
            if (compare_and_swap(&dist_next[v], INF, level)) {
                frontier_next[v] = v;
            }
        }
    }
    
    barrier();
    auto time_2 = chrono::system_clock::now();
    chrono::duration<double> delta = (time_2 - time_1);
    if (DEBUG && rank_me() == 0) cout << "Calculation: " << delta.count() << endl;
    
    sync_round_sparse(g, dist_next, frontier_next);
    auto time_3 = chrono::system_clock::now();
    delta = time_3 - time_2;
    if (DEBUG && rank_me() == 0) cout << "Communication: " << delta.count() << endl;
    frontier_size = sequence::filter(frontier_next, frontier, g.num_nodes, nonNegF());
    return frontier_size; 
}

void sync_round_dense(Graph& g, Distance* dist_next, bool* frontier_next) {  
    # pragma omp parallel for
    for (VertexId i = 0; i < rank_n(); i++) {
        broadcast(dist_next+g.rank_start_node(i), g.rank_num_nodes(i), i).wait();
        broadcast(frontier_next+g.rank_start_node(i), g.rank_num_nodes(i), i).wait();
    }
    barrier();
}

VertexId bfs_dense(Graph& g, global_ptr<Distance> dist_dist, global_ptr<Distance> dist_next_dist, global_ptr<bool> frontier_dist, global_ptr<bool> frontier_next_dist, VertexId level) {
    auto time_1 = chrono::system_clock::now();
    Distance* dist = dist_dist.local();
    Distance* dist_next = dist_next_dist.local();
    bool* frontier = frontier_dist.local();
    bool* frontier_next = frontier_next_dist.local();

    # pragma omp parallel for
    for (VertexId u = 0; u < g.num_nodes; u++) {
        // update next round of dist
        dist_next[u] = dist[u];
        frontier_next[u] = false;
    }

    # pragma omp parallel for
    for (VertexId u = g.rank_start; u < g.rank_end; u++) {
        // ignore if distance is set already
        if (dist_next[u] != INF) continue;
        VertexId* neighbors = g.in_neighbors(u).local(); 

        for (EdgeId j = 0; j < g.in_degree(u); j++) {
            VertexId v = neighbors[j];

            if (!frontier[v]) continue;

            if (!frontier_next[u]) {
                dist_next[u] = level; 
                frontier_next[u] = true;
            }
        }

    }
    barrier();
    auto time_2 = chrono::system_clock::now();
    chrono::duration<double> delta = (time_2 - time_1);
    if (DEBUG && rank_me() == 0) cout << "Calculation: " << delta.count() << endl;
    sync_round_dense(g, dist_next, frontier_next);
    auto time_3 = chrono::system_clock::now();
    delta = time_3 - time_2;
    if (DEBUG && rank_me() == 0) cout << "Communication: " << delta.count() << endl;
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

Distance* bfs(Graph &g, VertexId root) {
    // https://github.com/sbeamer/gapbs/blob/master/src/pr.cc
    global_ptr<Distance> dist_dist = new_array<Distance>(g.num_nodes); Distance* dist = dist_dist.local();
    global_ptr<Distance> dist_next_dist = new_array<Distance>(g.num_nodes); Distance* dist_next = dist_next_dist.local();

    global_ptr<VertexId> frontier_sparse_dist = new_array<VertexId>(g.num_nodes); VertexId* frontier_sparse = frontier_sparse_dist.local();
    global_ptr<VertexId> frontier_sparse_next_dist = new_array<VertexId>(g.num_nodes); VertexId* frontier_sparse_next = frontier_sparse_next_dist.local();

    global_ptr<bool> frontier_dense_dist = new_array<bool>(g.num_nodes); bool* frontier_dense = frontier_dense_dist.local();
    global_ptr<bool> frontier_dense_next_dist = new_array<bool>(g.num_nodes); bool* frontier_dense_next = frontier_dense_next_dist.local();

    # pragma omp parallel for
    for (VertexId i = 0; i < g.num_nodes; i++) {
        dist[i] = INF;
    }

    bool is_sparse_mode = true;

    frontier_sparse[0] = root;
    VertexId frontier_size = 1;
    dist[root] = 0; // initialize everyone to INF except root

    VertexId level = 0;
    const int threshold_fraction_denom = 20;

    while (frontier_size != 0) {
        level++; 
        bool should_be_sparse_mode = frontier_size < (g.num_nodes / threshold_fraction_denom);

        if (DEBUG && rank_me() == 0) cout << "Round " << level << " | " << "Frontier: " << frontier_size << " | Sparse? " << should_be_sparse_mode << endl;
        auto time_before = chrono::system_clock::now();

        if (should_be_sparse_mode) {
            if (!is_sparse_mode) {
                dense_to_sparse(frontier_dense, g.num_nodes, frontier_sparse);
            }
            is_sparse_mode = true;
            frontier_size = bfs_sparse(g, dist_dist, dist_next_dist, frontier_sparse_dist, frontier_sparse_next_dist, frontier_size, level);
        } else {
            if (is_sparse_mode) {
                sparse_to_dense(frontier_sparse, frontier_size, frontier_dense);
            }
            is_sparse_mode = false;
            frontier_size = bfs_dense(g, dist_dist, dist_next_dist, frontier_dense_dist, frontier_dense_next_dist, level);

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

/*
bool verify(Graph& g, int root, vector<int> dist_in) {
    vector<int> dist(g.num_nodes);

    if (dist.size() != dist_in.size())
        return false;

    for (int i = 0; i < g.num_nodes; i++)
        dist[i] = INF; // set INF

    dist[root] = 0;
    int level = 1;
    vector<int> frontier, next_frontier;
    frontier.push_back(root);
    while (frontier.size() != 0) {
        for (int i = 0; i < frontier.size(); i++) {
            int u = frontier[i];
            vector<int> neighbors = g.out_neighbors(u);
            for (int j = 0; j < g.out_degree(u); j++) {
                int v = neighbors[j];
                if (dist[v] == INF) {
                    next_frontier.push_back(v);
                    dist[v] = level;
                }
            }
        }
        frontier = next_frontier; 
        next_frontier.clear();
        level += 1; 
    }

    for (int i = 0; i < dist.size(); i++) {
        if (dist[i] != dist_in[i]) {
            return false;
        }
    }
    return true;
}*/

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Usage: ./bfs <path_to_graph> <num_iters>" << endl;
        exit(-1);
    }

    init();

    Graph g = Graph(argv[1]);
    int num_iters = atoi(argv[2]);

    barrier(); 
    srand(time(NULL));
    float current_time = 0.0;
    for (int i = 0; i < num_iters; i++) {
        VertexId root = rand() % g.num_nodes;
        root = broadcast(root, 0).wait();
        auto time_before = std::chrono::system_clock::now();
        Distance* dist = bfs(g, root);
        auto time_after = std::chrono::system_clock::now();
        std::chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
        /*if (rank_me() == 0) {
            for (VertexId i = 0; i < g.num_nodes; i++)
                cout << dist[i] << endl;
        }*/
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