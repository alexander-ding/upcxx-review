#include "graph.hpp"
#include "upcxx/upcxx.hpp"
#include <chrono>
#include <ctime> 
#include <climits>
#include <stdlib.h> 
#include <time.h>

using namespace upcxx;
typedef VertexId Distance;
Distance* bfs(Graph &g, VertexId root) {
    return
    g.compute<Distance>(
        [&](VertexId i) -> Distance {
            if (i != root) {
                return INF;
            } else{
                return 0;
            }
        },
        [&](VertexId i) -> bool {
            return i == root;
        },
        [&](Distance* dist, Distance* dist_next, VertexId* frontier_next, VertexId u, VertexId v, VertexId level) {
            if (dist_next[v] == INF) {
                dist_next[v] = level;
                frontier_next[v] = v;
            }
        },
        [&](Distance* dist, Distance* dist_next, bool* frontier_next, VertexId u, VertexId v, VertexId level) {
            if (dist_next[u] == INF && !frontier_next[u]) {
                dist_next[u] = level;
                frontier_next[u] = true;
            }
        }
    );
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
        VertexId root = rand() % g.num_nodes;
        root = broadcast(root, 0).wait();
        auto time_before = std::chrono::system_clock::now();
        Distance* dist = bfs(g, root);
        auto time_after = std::chrono::system_clock::now();
        std::chrono::duration<double> delta_time = time_after - time_before;
        current_time += delta_time.count();
        /*if (rank_me() == 0) {
            for (VertexId i = 0; i < g.num_nodes; i++)
                cout << dist[i] << endl;
        }*/
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