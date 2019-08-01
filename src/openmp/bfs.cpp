#include <omp.h>

#include <iostream>
#include <chrono>
#include <ctime> 

#include <vector>
#include <queue>
#include <climits>

#include "graph.hpp"

using namespace std;

template <typename T>
void print_vector(const vector<T> & v) {
    cout << endl;
    for (auto i = v.begin(); i != v.end(); ++i) {
        cout << *i << " ";
    }
    cout << endl;
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
        frontier = next_frontier; 
        next_frontier.clear();
        level += 1; 
    }

    return dist;
}

bool verify(Graph& g, int root, vector<int> dist_in) {
    vector<int> dist(g.num_nodes());

    if (dist.size() != dist_in.size())
        return false;

    for (int i = 0; i < g.num_nodes(); i++)
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
        cout << "Usage: ./bfs <path_to_graph> <root_id>" << endl;
        exit(-1);
    }

    int root = atoi(argv[2]);
    
    Graph g(argv[1]);
    auto time_before = chrono::system_clock::now();
    vector<int> scores = bfs(g, root);

    auto time_after = chrono::system_clock::now();
    chrono::duration<double> delta_time = time_after - time_before;
    cout << delta_time.count() << endl;

    /* if (!verify(g, root, scores)) {
        cerr << "BFS result is not correct" << endl;
    } */
}