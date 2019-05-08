#include <iostream>
#include <cstdlib>
#include <chrono>
#include <ctime> 
#include <vector>
#include <cmath>

#include "upcxx/upcxx.hpp"
#include "dvector.hpp"

using namespace std;
using namespace upcxx;

#include "graph.hpp"

using namespace std;

const float damp = 0.85;

vector<float> pagerank(Graph &g, int max_iters, float epsilon=0) {
    // https://github.com/sbeamer/gapbs/blob/master/src/pr.cc
    int n_nodes = g.num_nodes();
    float init_score = 1.0f / n_nodes;
    float base_score = (1.0f - damp) / n_nodes;
    global_ptr<float> scores = nullptr;
    global_ptr<float> outgoing_contrib = nullptr;
    global_ptr<float> errors = nullptr;
    if (rank_me() == 0) {
        scores = new_array<float>(n_nodes);
        outgoing_contrib = new_array<float>(n_nodes);
        errors = new_array<float>(rank_n());
        future<> f = make_future();
        for (int i=0; i < n_nodes; i++) {
            f = when_all(f, rput(init_score, scores+i));
        }
        f.wait();
    }
    barrier();
    scores = broadcast(scores, 0).wait();
    outgoing_contrib = broadcast(outgoing_contrib, 0).wait();
    errors = broadcast(errors, 0).wait();
    
    for (int iter = 0; iter < max_iters; iter++) {
        // first calculate outgoing contrib
        for (int n = rank_me(); n < n_nodes; n=n+rank_n()) {
            auto score_n = rget(scores+n).wait();
            auto val = score_n / g.out_degree(n);
            rput(val, outgoing_contrib+n).wait();
        }
        barrier();
        // then update scores
        float error = 0.0f;
        for (int n = rank_me(); n < n_nodes; n=n+rank_n()) {
            float incoming_total = 0.0;
            for (int v : g.in_neighbors(n))
                incoming_total += rget(outgoing_contrib+v).wait();
            float old_score = rget(scores+n).wait();
            rput(base_score + damp * incoming_total, scores+n).wait();
            error += fabs(rget(scores+n).wait() - old_score);
        }
        rput(error, errors+rank_me()).wait();
        float total_error = 0.0f;
        barrier();
        // check if error breaks
        if (rank_me()==0) {
            float *local_errors = errors.local();
            for (int i = 0; i < rank_n(); i++) {
                total_error += local_errors[i];
            }
        }
        barrier();
        if (total_error < epsilon)
            break;
    }
    return vector<float>(scores.local(), scores.local()+n_nodes);
}

bool verify(vector<float> &scores_compare, Graph &g, int max_iters, float epsilon=0.0) {
    float init_score = 1.0f / g.num_nodes();
    float base_score = (1.0f - damp) / g.num_nodes();
    vector<float> scores(g.num_nodes(), init_score);
    vector<float> incoming_sums(g.num_nodes(), 0.0);

    for (int iter = 0; iter < max_iters; iter++) {
        double error = 0;

        for (int n = 0; n < g.num_nodes(); n++) {
            float outgoing_contrib = scores[n] / g.out_degree(n);
            for (int v : g.out_neighbors(n))
                incoming_sums[v] += outgoing_contrib;
        }
        for (int n = 0; n < g.num_nodes(); n++) {
            float old_score = scores[n];
            scores[n] = base_score + damp * incoming_sums[n];
            error += fabs(old_score - scores[n]);
            incoming_sums[n] = 0.0; // reset
        }
        if (error < epsilon)
            break;
    }

    double error_between_scores = 0.0f;
    for (int n = 0; n < g.num_nodes(); n++) {
        error_between_scores += fabs(scores[n] - scores_compare[n]);
    }

    cout << "Total Error: " << error_between_scores << endl;
    return error_between_scores < epsilon;
}


int main(int argc, char *argv[]) {
    upcxx::init();

    Graph g;
    if (rank_me() == 0) {
        if (argc != 2) {
            cout << "Usage: ./pagerank <path_to_graph>" << endl;
            exit(-1);
        }
        g.populate(argv[1]);
    }

    barrier();

    int max_iters = 10;
    const float tolerance = 1e-4;
    auto time_before = std::chrono::system_clock::now();
    auto scores = pagerank(g, max_iters);

    barrier();

    auto time_after = std::chrono::system_clock::now();
    std::chrono::duration<double> delta_time = time_after - time_before;
    
    if (rank_me()==0) {
        std::cout << "Time: " << delta_time.count() << "s" << std::endl;
        /*// also verify
        if (verify(scores, g, max_iters, tolerance)) {
            cout << "Success!" << endl;
        } else {
            cout << "Fails!" << endl;
        }*/
    }
    upcxx::finalize();
}