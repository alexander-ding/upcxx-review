#include <iostream>
#include <omp.h>
#include <cstdlib>
#include <chrono>
#include <ctime> 
#include <vector>
#include <cmath>

#include "graph.hpp"

using namespace std;

template<typename T>
void print(std::vector<T> const &input)
{ 
    for (int i = 0; i < input.size(); i++) {
        cout << input.at(i) << ' ';
    }
    cout << endl;
}


const float damp = 0.85;

vector<float> pagerank(const Graph &g, int max_iters, float epsilon=0) {
    // https://github.com/sbeamer/gapbs/blob/master/src/pr.cc
    float init_score = 1.0f / g.num_nodes();
    float base_score = (1.0f - damp) / g.num_nodes();
    vector<float> scores(g.num_nodes(), init_score);
    vector<float> outgoing_contrib(g.num_nodes());
    
    for (int iter = 0; iter < max_iters; iter++) {
        double error = 0; 
        # pragma omp parallel for
        for (int n = 0; n < g.num_nodes(); n++) {
            outgoing_contrib[n] = scores[n] / g.out_degree(n);
        }
        #pragma omp parallel for reduction(+ : error) schedule(dynamic, 64)
        for (int n = 0; n < g.num_nodes(); n++) {
            float incoming_total = 0.0;
            for (float v : g.in_neighbors(n))
                incoming_total += outgoing_contrib[v];
            float old_score = scores[n];
            scores[n] = base_score + damp * incoming_total;
            error += fabs(scores[n] - old_score);
        }
        if (error < epsilon)
            break;
    }
    return scores;
}
/*
bool verify(const vector<float> &scores_compare, const Graph &g, int max_iters, float epsilon=0.0) {
    float init_score = 1.0f / g.num_nodes();
    float base_score = (1.0f - damp) / g.num_nodes();
    vector<float> scores(g.num_nodes(), init_score);
    vector<float> outgoing_contrib(g.num_nodes());

    for (int iter = 0; iter < max_iters; iter++) {
        double error = 0;

        for (int n = 0; n < g.num_nodes(); n++)
            outgoing_contrib[n] = scores[n] / g.out_degree(n);
        for (int n = 0; n < g.num_nodes(); n++) {
            float incoming_total = 0.0;
            for (float v : g.in_neighbors(n))
                incoming_total += outgoing_contrib[v];
            float old_score = scores[n];
            scores[n] = base_score + damp * incoming_total;
            error += fabs(old_score - scores[n]);
        }
        if (error < epsilon)
            break;
    }
    print(scores);
    double error_between_scores = 0.0f;
    for (int n = 0; n < g.num_nodes(); n++) {
        error_between_scores += fabs(scores[n] - scores_compare[n]);
    }

    cout << "Total Error: " << error_between_scores << endl;
    return error_between_scores < 1.0;
}*/

bool verify(const vector<float> &scores_compare, const Graph &g, int max_iters, float epsilon=0.0) {
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
    if (argc != 2) {
        cout << "Usage: ./pagerank <path_to_graph>" << endl;
        exit(-1);
    }

    const float tolerance = 1e-4;
    const int max_iters = 100;

    Graph g(argv[1]);
    vector<float> scores = pagerank(g, max_iters, tolerance);
    if (verify(scores, g, max_iters, tolerance)) {
        cout << "Succeeds" << endl;
    } else {
        cout << "Fails" << endl;
    }
}