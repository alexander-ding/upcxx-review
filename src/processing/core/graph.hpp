#ifndef GRAPH_HPP
#define GRAPH_HPP

#include <vector>
#include <fstream>
#include <upcxx/upcxx.hpp>
#include "utils.hpp"
#include "sequence.hpp"
#include <stdlib.h>
#include <chrono>
#include <ctime>
#include <functional>


using namespace std;
using namespace upcxx;

typedef long VertexId;
typedef long EdgeId;
const long INF = LONG_MAX;

class Graph {
    global_ptr<EdgeId> out_offsets_dist;
    EdgeId* out_offsets;
    global_ptr<VertexId> out_edges_dist; 
    VertexId* out_edges;

    global_ptr<EdgeId> in_offsets_dist;
    EdgeId* in_offsets;
    global_ptr<VertexId> in_edges_dist;
    VertexId* in_edges;
    
    public:
        VertexId num_nodes;
        VertexId num_nodes_local;
        EdgeId num_edges;
        EdgeId num_out_edges_local; 
        EdgeId num_in_edges_local;

        VertexId rank_start;
        VertexId rank_end;
        Graph(char* path);
        template<typename EdgeData>
        EdgeData* compute(
                std::function<EdgeData(VertexId)> init_d, 
                std::function<bool(VertexId)> init_frontier,
                std::function<void(EdgeData*, EdgeData*, VertexId*, VertexId, VertexId, VertexId)> sparse_step = nullptr,
                std::function<void(EdgeData*, EdgeData*, bool*, VertexId, VertexId, VertexId)> dense_step = nullptr
        ) {
            if (!sparse_step && !dense_step) {
                cerr << "Must supply at least one of sparse_step and dense_step" << endl;
                exit(-1);
            }
            global_ptr<EdgeData> d_dist = new_array<VertexId>(num_nodes); EdgeData* d = d_dist.local();
            global_ptr<EdgeData> d_next_dist = new_array<VertexId>(num_nodes); EdgeData* d_next = d_next_dist.local();
            
            global_ptr<EdgeData> frontier_sparse_dist = new_array<VertexId>(num_nodes); EdgeData* frontier_sparse = frontier_sparse_dist.local();
            global_ptr<EdgeData> frontier_sparse_next_dist = new_array<VertexId>(num_nodes); EdgeData* frontier_sparse_next = frontier_sparse_next_dist.local();

            global_ptr<bool> frontier_dense_dist = new_array<bool>(num_nodes); bool* frontier_dense = frontier_dense_dist.local();
            global_ptr<bool> frontier_dense_next_dist = new_array<bool>(num_nodes); bool* frontier_dense_next = frontier_dense_next_dist.local();

            VertexId frontier_size = 0;
            for (VertexId i = 0; i < num_nodes; i++) {
                d[i] = INF;
                frontier_sparse[i] = -1;
            }
            for (VertexId i = rank_start; i < rank_end; i++) {
                d[i] = init_d(i);
                if (init_frontier(i)) {
                    frontier_sparse[frontier_size] = i;
                    frontier_size += 1;
                }
            }
            sync_round_sparse(d, frontier_sparse);
            frontier_size = sequence::filter(frontier_sparse, frontier_sparse, num_nodes, nonNegF());
            
            bool is_sparse_mode = true;
            VertexId level = 0;
            const int threshold_fraction_denom = 20; 

            while (frontier_size != 0) {
                level++;
                bool should_be_sparse_mode = (frontier_size < (num_nodes / threshold_fraction_denom) && !sparse_step);

                if (DEBUG && rank_me() == 0) cout << "Round " << level << " | " << "Frontier: " << frontier_size << " | Sparse? " << should_be_sparse_mode << endl;

                auto time_before = chrono::system_clock::now();

                if (should_be_sparse_mode) {
                    if (!is_sparse_mode) {
                        dense_to_sparse(frontier_dense, frontier_sparse);
                    }
                    is_sparse_mode = true;
                    auto time_1 = chrono::system_clock::now();
                    for (VertexId i = 0; i < num_nodes; i++) {
                        d_next[i] = d[i];
                        frontier_sparse_next[i] = -1;
                    }

                    for (VertexId i = 0;i < frontier_size; i++) {
                        VertexId u = frontier_sparse[i];
                        if (!(rank_start <= u && u < rank_end)) continue;
                        VertexId* neighbors = out_neighbors(u).local();
                        for (EdgeId j = 0; j < out_degree(u); j++) {
                            VertexId v = neighbors[j];
                            sparse_step(d, d_next, frontier_sparse_next, u, v, level);
                        }
                    }
                    barrier();
                    auto time_2 = chrono::system_clock::now();
                    chrono::duration<double> delta = time_2 - time_1;
                    if (DEBUG && rank_me() == 0) cout << "Calculation: " << delta.count() << endl;

                    sync_round_sparse(d_next, frontier_sparse_next);
                    auto time_3 = chrono::system_clock::now();
                    delta = time_3 - time_2; 
                    if (DEBUG && rank_me() == 0) cout << "Communication: " << delta.count() << endl;
                    frontier_size = sequence::filter(frontier_sparse_next, frontier_sparse, num_nodes, nonNegF());
                } else {
                    if (is_sparse_mode) {
                        sparse_to_dense(frontier_sparse, frontier_size, frontier_dense);
                    }
                    is_sparse_mode = false;
                    
                    auto time_1 = chrono::system_clock::now();
                    for (VertexId u = 0; u < num_nodes; u++) {
                        // update next round of values
                        d_next[u] = d[u];
                        frontier_dense_next[u] = false;
                    }


                    for (VertexId u = rank_start; u < rank_end; u++) {
                        VertexId* neighbors = in_neighbors(u).local();

                        for (EdgeId j = 0; j < in_degree(u); j++) {
                            VertexId v = neighbors[j];

                            if (!frontier_dense[v]) continue;

                            dense_step(d, d_next, frontier_dense_next, u, v, level);
                        }
                    }
                    barrier();
                    auto time_2 = chrono::system_clock::now();
                    chrono::duration<double> delta = time_2 - time_1;
                    if (DEBUG && rank_me() == 0) cout << "Calculation: " << delta.count() << endl;
                    
                    sync_round_dense(d_next, frontier_dense_next);
                    auto time_3 = chrono::system_clock::now();
                    delta = time_3 - time_2; 
                    if (DEBUG && rank_me() == 0) cout << "Communication: " << delta.count() << endl;

                    frontier_size = sequence::sumFlagsSerial(frontier_dense_next, num_nodes);
                    
                    swap(frontier_dense_next_dist, frontier_dense_dist);
                    swap(frontier_dense_next, frontier_dense);
                }

                swap(d_next_dist, d_dist);
                swap(d_next, d);
                barrier();

                auto time_after = chrono::system_clock::now();
                chrono::duration<double> delta = time_after - time_before;
                if (DEBUG && rank_me() == 0) cout << "Time: " << delta.count() << endl;
            }

            delete_array(d_next_dist); delete_array(frontier_sparse_dist); delete_array(frontier_sparse_next_dist); delete_array(frontier_dense_dist); delete_array(frontier_dense_next_dist);

            return d;
        }
        template<typename EdgeData>
        void sync_round_sparse(EdgeData* d_next, VertexId* frontier_next) {
            promise<> p;
            reduce_all(d_next, d_next, num_nodes, op_fast_min, world(), operation_cx::as_promise(p));
            reduce_all(frontier_next, frontier_next, num_nodes, op_fast_max, world(), operation_cx::as_promise(p));
            p.finalize().wait();
            barrier();
        }

        template<typename EdgeData>
        void sync_round_dense(EdgeData* d_next, bool* frontier_next) {
            for (VertexId i = 0; i < rank_n(); i++) {
                broadcast(d_next+rank_start_node(i), rank_num_nodes(i), i).wait();
                broadcast(frontier_next+rank_start_node(i), rank_num_nodes(i), i).wait();
            }
            barrier();
        }
    
        int vertex_rank(const VertexId n);
        EdgeId in_degree(const VertexId n);
        EdgeId out_degree(const VertexId n);

        global_ptr<VertexId> in_neighbors(const VertexId n);
        global_ptr<VertexId> out_neighbors(const VertexId n);
        
        inline VertexId rank_start_node(const int n) { 
            return int(num_nodes / rank_n() * n);
        }
        inline VertexId rank_end_node(const int n) {
            return (n == rank_n() - 1) ? (num_nodes) : (int(num_nodes / rank_n() * (n+1)));
        }
        inline VertexId rank_num_nodes(const int n) {
            return rank_end_node(n) - rank_start_node(n);
        }

        
        void sparse_to_dense(VertexId* frontier_sparse, VertexId frontier_size, bool* frontier_dense) {
            for (VertexId i = 0; i < frontier_size; i++) {
                frontier_dense[frontier_sparse[i]] = true;
            }
        }

        void dense_to_sparse(bool* frontier_dense, VertexId* frontier_sparse) {
            for (VertexId i = 0; i < num_nodes; i++) {
                if (frontier_dense[i]) {
                    frontier_sparse[i] = i;
                } else {
                    frontier_sparse[i] = -1;
                }
            }
            sequence::filter(frontier_sparse, frontier_sparse, num_nodes, nonNegF());
        }
};

Graph::Graph(char *path) {
    if (!file_exists(path)) {
        if (rank_me() == 0)
            cout << "Graph file does not exist" << endl;
        abort();
    }
    ifstream fin(path);
    VertexId n; EdgeId m;
    fin >> n >> m;
    num_nodes = n; num_edges = m;
    
    rank_start = int(n / rank_n() * rank_me());
    if (rank_me() == rank_n() - 1) {
        rank_end = n;
    } else {
        rank_end = int(n / rank_n() * (rank_me()+1));
    }

    num_nodes_local = rank_end-rank_start;

    out_offsets_dist = new_array<EdgeId>(num_nodes_local);
    in_offsets_dist = new_array<EdgeId>(num_nodes_local);
    out_offsets = out_offsets_dist.local();
    in_offsets = in_offsets_dist.local();

    EdgeId offset;
    VertexId edge; 
    EdgeId offset_start;
    EdgeId offset_end = -1;

    // loop through and ignore all non-local nodes
    for (VertexId i = 0; i < n; i++) {
        fin >> offset;
        if (i == rank_start) {
            offset_start = offset;
        }
        if (i >= rank_start && i < rank_end)
            out_offsets[i-rank_start] = offset-offset_start;
        if (i == rank_end)
            offset_end = offset;
    }
    if (offset_end == -1) // if unset, set it
        offset_end = m;

    num_out_edges_local = offset_end - offset_start;
    out_edges_dist = new_array<VertexId>(num_out_edges_local);
    out_edges = out_edges_dist.local();

    for (EdgeId i = 0; i < m; i++) {
        fin >> edge;
        if (i >= offset_start && i < offset_end) {
            out_edges[i-offset_start] = edge;
        }
    }

    // loop through and ignore all non-local nodes
    for (VertexId i = 0; i < n; i++) {
        fin >> offset;
        if (i == rank_start) {
            offset_start = offset;
        }
        if (i >= rank_start && i < rank_end)
            in_offsets[i-rank_start] = offset-offset_start;
        if (i == rank_end)
            offset_end = offset;
    }
    if (offset_end == -1) // if unset, set it
        offset_end = m;

    num_in_edges_local = offset_end - offset_start;
    in_edges_dist = new_array<VertexId>(num_in_edges_local);
    in_edges = in_edges_dist.local();
    for (EdgeId i = 0; i < m; i++) {
        fin >> edge;
        if (i >= offset_start && i < offset_end) {
            in_edges[i-offset_start] = edge;
        }
    }
}

int Graph::vertex_rank(const VertexId n) {
    return int(n / (num_nodes / rank_n()));
}

EdgeId Graph::in_degree(const VertexId n)  {
    assert((n >= rank_start) && (n < rank_end));
    if ((n - rank_start) == (num_nodes_local-1)) {
        return num_in_edges_local - in_offsets[num_nodes_local-1];
    }
    return in_offsets[(n-rank_start)+1] - in_offsets[n-rank_start];
}

EdgeId Graph::out_degree(const VertexId n)  {
    assert((n >= rank_start) && (n < rank_end));
    if ((n - rank_start) == (num_nodes_local-1)) {
        return num_out_edges_local - out_offsets[num_nodes_local-1];
    }
    return out_offsets[(n-rank_start)+1] - out_offsets[n-rank_start];
}

global_ptr<VertexId> Graph::in_neighbors(const VertexId n) {
    assert((n >= rank_start) && (n < rank_end));
    return in_edges_dist + in_offsets[n-rank_start];
}

global_ptr<VertexId> Graph::out_neighbors(const VertexId n) {
    assert((n >= rank_start) && (n < rank_end));
    return out_edges_dist + out_offsets[n-rank_start];
}

#endif