#include <iostream>
#include <omp.h>
#include <cstdlib>
#include <chrono>
#include <ctime> 
#include <vector>
#include <cmath>

#include "graph.hpp"
#include "sequence.hpp"
#include "utils.hpp"

using namespace std;

const double damp = 0.85;

double pagerank_dense(Graph& g, double* scores, double* scores_next, double* errors, double* outgoing_contrib, int level) {
    double base_score = (1.0 - damp) / g.num_nodes();

    # pragma omp parallel for
    for (int n = 0; n < g.num_nodes(); n++)
        outgoing_contrib[n] = scores[n] / g.out_degree(n);

    # pragma omp parallel for
    for (int u = 0; u < g.num_nodes(); u++) {
        double sum = 0;
        vector<int> neighbors = g.in_neighbors(u);
        for (int j = 0; j < g.in_degree(u); j++) {
            int v = neighbors[j];
            sum += outgoing_contrib[v];
        }
        scores_next[u] = base_score + damp * sum;
        errors[u] = fabs(scores_next[u] - scores[u]);
    }

    double delta = sequence::plusScan(errors, errors, g.num_nodes());

    return delta;
}

double* pagerank(Graph& g, int num_iters) {
    double* scores = newA(double, g.num_nodes());
    double* scores_next = newA(double, g.num_nodes());
    double* errors = newA(double, g.num_nodes());
    double* outgoing_contrib = newA(double, g.num_nodes());
    double error = 0.0;

    double init_score = 1.0 / g.num_nodes();
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++) {
        scores[i] = init_score; // set init score
    }

    int level = 0;
    const int threshold_fraction_denom = 20; 

    while (level < num_iters) {
        level++; 
        // no decision needed, since it's always better
        // to run pagerank as dense
        error = pagerank_dense(g, scores, scores_next, errors, outgoing_contrib, level);
        swap(scores, scores_next);
    }
    free(scores_next); free(errors); free(outgoing_contrib);
    return scores;
}

bool verify(const double* scores_compare, const Graph &g, int max_iters) {
    double init_score = 1.0 / g.num_nodes();
    double base_score = (1.0 - damp) / g.num_nodes();
    vector<double> scores(g.num_nodes(), init_score);
    vector<double> incoming_sums(g.num_nodes(), 0.0);

    for (int iter = 0; iter < max_iters; iter++) {
        double error = 0;

        for (int n = 0; n < g.num_nodes(); n++) {
            double outgoing_contrib = scores[n] / g.out_degree(n);
            for (int v : g.out_neighbors(n))
                incoming_sums[v] += outgoing_contrib;
        }
        for (int n = 0; n < g.num_nodes(); n++) {
            double old_score = scores[n];
            scores[n] = base_score + damp * incoming_sums[n];
            error += fabs(old_score - scores[n]);
            incoming_sums[n] = 0.0; // reset
        }
    }

    double error_between_scores = 0.0;
    for (int n = 0; n < g.num_nodes(); n++) {
        error_between_scores += fabs(scores[n] - scores_compare[n]);
    }

    cout << "Total Error: " << error_between_scores << endl;
    double epsilon = 1e-5;
    if (error_between_scores > epsilon) {
        cout << "Wrong!" << endl;
        return false;
    }
    return true;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Usage: ./pagerank <path_to_graph> <num_iters>" << endl;
        exit(-1);
    }

    const int max_iters = 10;

    Graph g(argv[1]);
    int num_iters = atoi(argv[2]);
    double current_time = 0.0;
    
    for (int i = 0; i < num_iters; i++) {
        auto time_before = std::chrono::system_clock::now();
        double* scores = pagerank(g, max_iters);

        auto time_after = std::chrono::system_clock::now();
        std::chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
        // verify(scores, g, max_iters);
    }
    std::cout << current_time / num_iters << std::endl;
    
    
}