#include "graph.hpp"
#include "upcxx/upcxx.hpp"
#include <chrono>
#include <ctime> 
#include <climits>
#include <stdlib.h> 
#include <time.h>
#include "sequence.hpp"

using namespace upcxx;

const double damp = 0.85;

void sync_round_dense(Graph& g, double* scores_next) {  
    for (VertexId i = 0; i < rank_n(); i++) {
        broadcast(scores_next+g.rank_start_node(i), g.rank_num_nodes(i), i).wait();
    }
    barrier();
}

double pagerank_dense(Graph& g, global_ptr<double> scores_dist, global_ptr<double> scores_next_dist, global_ptr<double> errors_dist, global_ptr<double> outgoing_contrib_dist, VertexId level) {
    double base_score = (1.0 - damp) / g.num_nodes;

    double* scores = scores_dist.local();
    double* scores_next = scores_next_dist.local();
    double* errors = errors_dist.local();
    double* outgoing_contrib = outgoing_contrib_dist.local();

    for (VertexId n = g.rank_start; n < g.rank_end; n++)
        outgoing_contrib[n] = scores[n] / g.out_degree(n);
    barrier();
    sync_round_dense(g, outgoing_contrib);

    for (VertexId u = g.rank_start; u < g.rank_end; u++) {
        double sum = 0;
        VertexId* neighbors = g.in_neighbors(u).local(); 

        for (EdgeId j = 0; j < g.in_degree(u); j++) {
            VertexId v = neighbors[j];
            sum += outgoing_contrib[v];
        }
        scores_next[u] = base_score + damp * sum;
        errors[u] = fabs(scores_next[u] - scores[u]);
    }
    barrier();
    sync_round_dense(g, scores_next);

    double delta = sequence::sum(errors, g.num_nodes);

    return delta;
}


double* pagerank(Graph &g, int num_iters) {
    global_ptr<double> scores_dist = new_array<double>(g.num_nodes); double* scores = scores_dist.local();
    global_ptr<double> scores_next_dist = new_array<double>(g.num_nodes); double* scores_next = scores_next_dist.local();

    global_ptr<double> errors_dist = new_array<double>(g.num_nodes); double* errors = errors_dist.local();
    global_ptr<double> outgoing_contrib_dist = new_array<double>(g.num_nodes); double* outgoing_contrib = outgoing_contrib_dist.local();

    double init_score = 1.0 / g.num_nodes;
    for (int i = 0; i < g.num_nodes; i++) {
        scores[i] = init_score; // set init score
    }

    VertexId level = 0;
    const int threshold_fraction_denom = 20;

    while (level < num_iters) {
        level++; 

        if (DEBUG && rank_me() == 0) cout << "Round " << level << endl;
        auto time_before = chrono::system_clock::now();

        pagerank_dense(g, scores_dist, scores_next_dist, errors_dist, outgoing_contrib_dist, level);

        swap(scores_next_dist, scores_dist);
        swap(scores_next, scores);
        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta = (time_after - time_before);
        if (DEBUG && rank_me() == 0) cout << "Time: " << delta.count() << endl;
    }

    delete_array(scores_next_dist); delete_array(errors_dist); delete_array(outgoing_contrib_dist); 

    return scores; 
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Usage: ./pagerank <path_to_graph> <num_iters>" << endl;
        exit(-1);
    }

    const int max_iters = 10;
    
    init();

    Graph g(argv[1]);
    int num_iters = atoi(argv[2]);

    barrier(); 
    float current_time = 0.0;
    for (int i = 0; i < num_iters; i++) {
        auto time_before = std::chrono::system_clock::now();
        double* scores = pagerank(g, max_iters);
        auto time_after = std::chrono::system_clock::now();
        std::chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
        /* if (rank_me() == 0) {
            for (int i = 0; i < g.num_nodes; i++)
                cout << scores[i] << endl;
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