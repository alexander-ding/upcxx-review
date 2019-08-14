#include "graph.hpp"
#include "upcxx/upcxx.hpp"
#include <chrono>
#include <ctime> 
#include <climits>

using namespace upcxx;
template <typename T>
void print_vector(const vector<T> & v) {
    cout << endl;
    for (auto i = v.begin(); i != v.end(); ++i) {
        cout << *i << " ";
    }
    cout << endl;
}


vector<int> connected_components(Graph &g) {
    // https://github.com/sbeamer/gapbs/blob/master/src/pr.cc

    dist_object<global_ptr<int>> labels(new_array<int>(g.num_nodes));
    dist_object<global_ptr<bool>> changed_last_round(new_array<bool>(g.num_nodes));
    auto labels_local = labels->local();
    auto changed_last_round_local = changed_last_round->local();
    for (int i = 0; i < g.num_nodes; i++) {
        labels_local[i] = i;
        changed_last_round_local[i] = true;
    }

    barrier();

    bool is_new_added = true; // is there new node edited

    while (is_new_added) {
        bool new_added = false;
        for (int u = g.rank_start; u < g.rank_end; u++) {
            vector<int> neighbors = g.in_neighbors(u);
            for (int j = 0; j < g.in_degree(u); j++) {
                int v = neighbors[j];
                // don't care about it if v hasn't been updated last round
                if (!changed_last_round_local[v]) continue; 
                if (labels_local[v] < labels_local[u]) {
                    labels_local[u] = labels_local[v];
                    changed_last_round_local[u] = true;
                    new_added = true;
                }
            }
        }

        barrier();

        // synchronize all
        global_ptr<int> root_labels = labels.fetch(0).wait();
        future<> fut_all_labels = make_future();
        for (int n = g.rank_start; n < g.rank_end; n++) {
            fut_all_labels = when_all(fut_all_labels, rput(labels_local[n], root_labels+n));
        }
        global_ptr<bool> root_changed_last = changed_last_round.fetch(0).wait();
        future<> fut_all_changed = make_future();
        for (int n = g.rank_start; n < g.rank_end; n++) {
            fut_all_changed = when_all(fut_all_changed, rput(changed_last_round_local[n], root_changed_last+n));
        }

        fut_all_labels.wait();
        fut_all_changed.wait();

        barrier();
        auto f_broadcast = upcxx::broadcast(labels_local, g.num_nodes, 0);
        f_broadcast = when_all(f_broadcast, upcxx::broadcast(changed_last_round_local, g.num_nodes, 0));
        f_broadcast.wait();

        is_new_added = reduce_all(new_added, op_bit_or).wait();
    };

    // ready the return
    vector<int> labels_return;
    labels_return.assign(labels_local, labels_local+g.num_nodes);
    return labels_return;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Usage: ./connected_components <path_to_graph> <num_iters>" << endl;
        exit(-1);
    }

    init();

    Graph g(argv[1]);
    int num_iters = atoi(argv[2]);
    barrier(); 
    float current_time = 0.0;
    for (int i = 0; i < num_iters; i++) {
        auto time_before = std::chrono::system_clock::now();
        vector<int> labels = connected_components(g);
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