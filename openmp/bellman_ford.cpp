#include <iostream>
#include <chrono>
#include <ctime> 
#include <omp.h>

#include <vector>
#include <queue>
#include <climits>

#include "graph_weighted.hpp"

using namespace std;

template <typename T>
void print_vector(const vector<T> & v) {
    cout << endl;
    for (auto i = v.begin(); i != v.end(); ++i) {
        cout << *i << " ";
    }
    cout << endl;
}

vector<int> bellman_ford(Graph& g, int root) {
    vector<int> dist(g.num_nodes());
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++)
        dist[i] = INT_MAX; // set INF

    dist[root] = 0;
    vector<int> frontier, next_frontier;
    frontier.push_back(root);

    int round = 0;
    while (frontier.size() != 0 && round < g.num_edges()) {
        #pragma omp parallel for
        for (int i = 0; i < frontier.size(); i++) {
            int u = frontier[i];
            vector<int> neighbors = g.out_neighbors(u);
            vector<int> weights = g.out_weights_neighbors(u);
            #pragma omp parallel for
            for (int j = 0; j < g.out_degree(u); j++) {
                int v = neighbors[j];
                int weight = weights[j];
                #pragma omp critical
                {
                if ((dist[u] + weight) < dist[v]) {         
                    dist[v] = dist[u] + weight;
                    next_frontier.push_back(v);
                }
                }
            }
        }
        frontier = next_frontier; 
        next_frontier.clear();
        round += 1;
    }

    if (round == (g.num_edges())) {
        cout << "There exists a negative-weight cycle" << endl;
    }

    return dist;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Usage: ./bellman_ford <path_to_graph> <root_id>" << endl;
        exit(-1);
    }

    const int root = atoi(argv[2]);
    
    Graph g(argv[1]);

    auto time_before = chrono::system_clock::now();
    vector<int> dists = bellman_ford(g, root);
    print_vector(dists);

    auto time_after = chrono::system_clock::now();
    chrono::duration<double> delta_time = time_after - time_before;
    cout << "Time: " << delta_time.count() << "s" << endl;
}