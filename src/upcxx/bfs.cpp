#include "graph.hpp"
#include "upcxx/upcxx.hpp"
#include <chrono>
#include <ctime> 
#include <climits>
#include <stdlib.h> 
#include <time.h>

using namespace upcxx;
template <typename T>
void print_vector(const vector<T> & v) {
    cout << endl;
    for (auto i = v.begin(); i != v.end(); ++i) {
        cout << *i << " ";
    }
    cout << endl;
}


void bfs_dense(Graph& g, dist_object<global_ptr<int>>& dist, dist_object<global_ptr<bool>>& frontier, dist_object<global_ptr<bool>>& next_frontier, int level) {
    auto dist_local = dist->local();
    auto frontier_local = frontier->local();
    auto next_frontier_local = next_frontier->local();
    global_ptr<bool> root_next_frontier = next_frontier.fetch(0).wait();
    global_ptr<int> root_dist = dist.fetch(0).wait();

    future<> fut_all = make_future();
    for (int n = g.rank_start; n < g.rank_end; n++) {
        // ignore if distance is already defined 
        if (dist_local[n] < level) continue;
        for (int v : g.in_neighbors(n)) {
            if (dist_local[v] == (level-1)) {
                fut_all = when_all(fut_all, rput(level, root_dist+n));
                fut_all = when_all(fut_all, rput(true, root_next_frontier+n));
                break;
            }
        }
    }

    barrier();

    fut_all.wait();
    barrier();
    upcxx::broadcast(next_frontier_local, g.num_nodes, 0).wait();
    upcxx::broadcast(dist_local, g.num_nodes, 0).wait();
    barrier();
}

void bfs_sparse(Graph& g, dist_object<global_ptr<int>>& dist, dist_object<global_ptr<bool>>& frontier, dist_object<global_ptr<bool>>& next_frontier, int level) {
    auto dist_local = dist->local();
    auto frontier_local = frontier->local();
    auto next_frontier_local = next_frontier->local();

    global_ptr<bool> root_next_frontier = next_frontier.fetch(0).wait();
    global_ptr<int> root_dist = dist.fetch(0).wait();

    future<> fut_all = make_future();
    for (int n = g.rank_start; n < g.rank_end; n++) {
        // skip any that's not in frontier
        if (!frontier_local[n]) continue;
        for (int v : g.out_neighbors(n)) {
            if (dist_local[v] == INT_MAX) {
                fut_all = when_all(fut_all, rput(level, root_dist+v));
                fut_all = when_all(fut_all, rput(true, root_next_frontier+v));
            }
        }
    }
    barrier();
    fut_all.wait();
    upcxx::broadcast(next_frontier_local, g.num_nodes, 0).wait();
    upcxx::broadcast(dist_local, g.num_nodes, 0).wait();
    barrier();
    
    
}

vector<int> bfs(Graph &g, int root) {
    // https://github.com/sbeamer/gapbs/blob/master/src/pr.cc

    dist_object<global_ptr<int>> dist(new_array<int>(g.num_nodes));
    auto dist_local = dist->local();
    for (int i = 0; i < g.num_nodes; i++) dist_local[i] = INT_MAX;
    dist_local[root] = 0; // initialize everyone to INF except root

    // is the vertex a frontier
    dist_object<global_ptr<bool>> frontier(new_array<bool>(g.num_nodes));
    auto frontier_local = frontier->local();
    for (int i = 0; i < g.num_nodes; i++) frontier_local[i] = false;
    frontier_local[root] = true;
    int frontier_size = 1;

    dist_object<global_ptr<bool>> next_frontier(new_array<bool>(g.num_nodes));
    auto next_frontier_local = next_frontier->local();
    for (int i = 0; i < g.num_nodes; i++) next_frontier_local[i] = false;
    barrier();

    int level = 1;
    while (frontier_size != 0) {
        if (frontier_size > (g.num_nodes / 20)) {
            bfs_dense(g, dist, frontier, next_frontier, level);
        } else {
            bfs_sparse(g, dist, frontier, next_frontier, level);
        }
        // set frontier_size
        // clear next_frontiers
        frontier_size = 0;
        for (int n = 0; n < g.num_nodes; n++) {
            frontier_local[n] = next_frontier_local[n];
            if (next_frontier_local[n]) {
                frontier_size += 1;
                next_frontier_local[n] = false;
            }
        }
        level += 1;
    }
    
    // ready the return
    vector<int> dist_return;
    dist_return.assign(dist_local, dist_local+g.num_nodes);
    return dist_return;
}

bool verify(Graph& g, int root, vector<int> dist_in) {
    vector<int> dist(g.num_nodes);

    if (dist.size() != dist_in.size())
        return false;

    for (int i = 0; i < g.num_nodes; i++)
        dist[i] = INT_MAX; // set INF

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
                if (dist[v] == INT_MAX) {
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
}

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
        dist_object<int> root(rand() % g.num_nodes);
        int root_local = root.fetch(0).wait();
        auto time_before = std::chrono::system_clock::now();
        vector<int> dist = bfs(g, root_local);
        // print_vector(dist);
        auto time_after = std::chrono::system_clock::now();
        std::chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
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