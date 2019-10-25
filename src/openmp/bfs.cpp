#include <omp.h>

#include <iostream>
#include <chrono>
#include <ctime> 

#include <vector>
#include <queue>
#include <climits>

#include "graph.hpp"

using namespace std;

const int THRESHOLD = 1000;

template <typename T>
void print_vector(const vector<T> & v) {
    cout << endl;
    for (auto i = v.begin(); i != v.end(); ++i) {
        cout << *i << " ";
    }
    cout << endl;
}

// BUG: next_frontier repeats elements (use a set instead)

void bfs_sparse(Graph& g, vector<int>& dist, vector<int>& frontier, vector<int>& next_frontier, int level) {
    #pragma omp parallel for
    for (int i = 0; i < frontier.size(); i++) {
        int u = frontier[i];
        vector<int> neighbors = g.out_neighbors(u);
        #pragma omp parallel for
        for (int j = 0; j < g.out_degree(u); j++) {
            int v = neighbors[j];
            if (dist[v] == INT_MAX) {
                #pragma omp critical
                next_frontier.push_back(v);

                dist[v] = level;
            }
        }
    }
}

void bfs_dense(Graph& g, vector<int>& dist, vector<int>& frontier, vector<int>& next_frontier, int level) {
    #pragma omp parallel for
    for (int u = 0; u < g.num_nodes(); u++) {
        // ignore if distance is set already
        if (dist[u] != INT_MAX) continue;
        vector<int> neighbors = g.in_neighbors(u);
        #pragma omp parallel for if (g.in_degree(u) > THRESHOLD)
        for (int j = 0; j < g.in_degree(u); j++) {
            int v = neighbors[j];
            if (dist[v] < (dist[u]-1)) {
                #pragma omp critical
                next_frontier.push_back(u);

                dist[u] = dist[v] + 1;
            }
        }
    }
}

vector<int> bfs(Graph& g, int root) {
    vector<int> dist(g.num_nodes());
    # pragma omp parallel for
    for (int i = 0; i < g.num_nodes(); i++)
        dist[i] = INT_MAX; // set INF

    dist[root] = 0;
    int level = 1;
    vector<int> frontier, next_frontier;
    frontier.push_back(root);
    while (frontier.size() != 0) {
        // for now use exclusively dense
        bfs_dense(g, dist, frontier, next_frontier, level);

        frontier = next_frontier; 
        next_frontier.clear();
        level += 1; 
    }

    return dist;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Usage: ./bfs <path_to_graph> <root_id>" << endl;
        exit(-1);
    }

    int num_iters = atoi(argv[2]);
    
    Graph g(argv[1]);

    float current_time = 0.0;
    srand(time(NULL));
    for (int i = 0; i < num_iters; i++) {
        int root = rand() % g.num_nodes();
        auto time_before = chrono::system_clock::now();
        vector<int> scores = bfs(g, root);
        auto time_after = chrono::system_clock::now();
        chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
    }
    cout << current_time / num_iters << endl;

    /* if (!verify(g, root, scores)) {
        cerr << "BFS result is not correct" << endl;
    } */
}