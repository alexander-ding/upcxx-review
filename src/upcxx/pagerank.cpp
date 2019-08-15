#include "graph.hpp"
#include "upcxx/upcxx.hpp"
#include <chrono>
#include <ctime> 

using namespace upcxx;
template <typename T>
void print_vector(const vector<T> & v) {
    cout << endl;
    for (auto i = v.begin(); i != v.end(); ++i) {
        cout << *i << " ";
    }
    cout << endl;
}

const float damp = 0.85;

vector<float> pagerank(Graph &g, int max_iters, float epsilon=0) {
    // https://github.com/sbeamer/gapbs/blob/master/src/pr.cc
    float init_score = 1.0f / g.num_nodes;
    float base_score = (1.0f - damp) / g.num_nodes;

    dist_object<global_ptr<float>> scores(new_array<float>(g.num_nodes));
    auto scores_local = scores->local();
    for (int i = 0; i < g.num_nodes; i++) scores_local[i] = init_score;

    dist_object<global_ptr<float>> outgoing_contrib(new_array<float>(g.num_nodes));
    
    for (int iter = 0; iter < max_iters; iter++) {
        double error = 0; 
        global_ptr<float> root_contrib = outgoing_contrib.fetch(0).wait();
        future<> fut_all = make_future();
        // compute local sections and add to rank 0
        for (int n = g.rank_start; n < g.rank_end; n++) {
            fut_all = when_all(fut_all, rput(scores_local[n] / g.out_degree(n), root_contrib+n));
        }
        fut_all.wait();
        barrier();
        
        // broadcast from rank 0
        float *local = outgoing_contrib->local();
        upcxx::broadcast(local, g.num_nodes, 0).wait();

        barrier();
        float *outgoing_contrib_local = outgoing_contrib->local();
        
        // now we compute scores
        for (int n = g.rank_start; n < g.rank_end; n++) {
            float incoming_total = 0.0;
            for (int v : g.in_neighbors(n)) {
                incoming_total += outgoing_contrib_local[v];
            }
            float old_score = scores_local[n];
            scores_local[n] = 0.0f;
            scores_local[n] = base_score + damp * incoming_total;
            error += fabs(scores_local[n] - old_score);
        }
        float total_error = reduce_all(error, op_fast_add).wait();
        
        if (total_error < epsilon)
            break;
    }
    global_ptr<float> root_score = scores.fetch(0).wait();
    future<> fut_all = make_future();
    for (int i = g.rank_start; i < g.rank_end; i++)
        fut_all = when_all(fut_all, rput(scores_local[i], root_score+i));
    fut_all.wait();
    barrier();
    float *local = scores->local();
    upcxx::broadcast(local, g.num_nodes, 0).wait();
    barrier();
    vector<float> scores_ret;
    scores_ret.assign(local, local+g.num_nodes);
    return scores_ret;
}


int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Usage: ./pagerank <path_to_graph> <num_iters>" << endl;
        exit(-1);
    }

    init();
    Graph g = Graph(argv[1]);
    int num_iters = atoi(argv[2]);
    barrier(); 

    float current_time = 0.0;
    for (int i = 0; i < num_iters; i++) {
        auto time_before = std::chrono::system_clock::now();
        vector<float> scores = pagerank(g, 10);
        auto time_after = std::chrono::system_clock::now();
        std::chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
    }
    
    if (rank_me() == 0) {
        std::cout << current_time / num_iters << std::endl;
    }
    finalize();
}