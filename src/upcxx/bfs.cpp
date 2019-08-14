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


vector<int> bfs(Graph &g, int root) {
    // https://github.com/sbeamer/gapbs/blob/master/src/pr.cc

    dist_object<global_ptr<int>> dist(new_array<int>(g.num_nodes));
    auto dist_local = dist->local();
    for (int i = 0; i < g.num_nodes; i++) dist_local[i] = INT_MAX;
    dist_local[root] = 0; // initialize everyone to INF except root
    barrier();

    bool is_new_added = true; // is there new node edited
    
    do {
        bool new_added = false;
        for (int n = g.rank_start; n < g.rank_end; n++) {
            // ignore if distance is already defined 
            if (dist_local[n] != INT_MAX) continue;
            for (int v : g.in_neighbors(n)) {
                if (dist_local[v] < (dist_local[n]-1)) {
                    dist_local[n] = dist_local[v] + 1;
                    new_added = true;
                }
            }
        }

        barrier();

        // synchronize all
        global_ptr<int> root_dist = dist.fetch(0).wait();
        future<> fut_all = make_future();
        for (int n = g.rank_start; n < g.rank_end; n++) {
            fut_all = when_all(fut_all, rput(dist_local[n], root_dist+n));
        }
        fut_all.wait();
        barrier();
        upcxx::broadcast(dist_local, g.num_nodes, 0).wait();

        is_new_added = reduce_all(new_added, op_bit_or).wait();
    } while (is_new_added);


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