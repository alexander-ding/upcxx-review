#include "graph_weighted.hpp"
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

bool compare(int v1, int w, int v2) {
    // checks if v1 + w < v2
    if (v1 == INT_MAX) return false;
    if (v2 == INT_MAX) return true;
    return ((v1+w) < v2);
}

vector<int> bellman_ford(Graph &g, int root) {
    // https://github.com/sbeamer/gapbs/blob/master/src/pr.cc

    dist_object<global_ptr<int>> dist(new_array<int>(g.num_nodes));
    dist_object<global_ptr<bool>> changed_last_round(new_array<bool>(g.num_nodes));
    auto dist_local = dist->local();
    auto changed_last_round_local = changed_last_round->local();
    for (int i = 0; i < g.num_nodes; i++) {
        dist_local[i] = INT_MAX;
        changed_last_round_local[i] = false;
    }
    dist_local[root] = 0; // initialize everyone to INF except root
    changed_last_round_local[root] = true;
    barrier();

    bool is_new_added = true; // is there new node edited
    int round = 0;
    
    while (is_new_added && round < g.num_nodes) {
        bool new_added = false;
        for (int u = g.rank_start; u < g.rank_end; u++) {
            vector<int> neighbors = g.in_neighbors(u);
            vector<int> weights = g.in_weights_neighbors(u);
            for (int j = 0; j < g.in_degree(u); j++) {
                int v = neighbors[j];
                // don't care about it if v hasn't been updated last round
                if (!changed_last_round_local[v]) continue; 
                int weight = weights[j];
                if (compare(dist_local[v], weight, dist_local[u])) {
                    dist_local[u] = dist_local[v] + weight;
                    changed_last_round_local[u] = true;
                    new_added = true;
                }
            }
        }

        barrier();

        // synchronize all
        global_ptr<int> root_dist = dist.fetch(0).wait();
        future<> fut_all_dist = make_future();
        for (int n = g.rank_start; n < g.rank_end; n++) {
            fut_all_dist = when_all(fut_all_dist, rput(dist_local[n], root_dist+n));
        }
        global_ptr<bool> root_changed_last = changed_last_round.fetch(0).wait();
        future<> fut_all_changed = make_future();
        for (int n = g.rank_start; n < g.rank_end; n++) {
            fut_all_changed = when_all(fut_all_changed, rput(changed_last_round_local[n], root_changed_last+n));
        }

        fut_all_dist.wait();
        fut_all_changed.wait();

        barrier();
        auto f_broadcast = upcxx::broadcast(dist_local, g.num_nodes, 0);
        f_broadcast = when_all(f_broadcast, upcxx::broadcast(changed_last_round_local, g.num_nodes, 0));
        f_broadcast.wait();

        is_new_added = reduce_all(new_added, op_bit_or).wait();
        round += 1;
    };

    // ready the return
    vector<int> dist_return;
    dist_return.assign(dist_local, dist_local+g.num_nodes);
    if (round == g.num_nodes) {
        //cout << "There exists a negative-weight cycle" << endl;
    }
    return dist_return;
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
        dist_object<int> root(rand() % g.num_nodes);
        int root_local = root.fetch(0).wait();
        auto time_before = std::chrono::system_clock::now();
        vector<int> dist = bellman_ford(g, root_local);
        auto time_after = std::chrono::system_clock::now();
        std::chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
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