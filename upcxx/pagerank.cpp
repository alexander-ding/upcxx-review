#include "graph.hpp"
#include "upcxx/upcxx.hpp"

using namespace upcxx;

void print_vector(vector<int> & v) {
    cout << endl;
    for (auto i = v.begin(); i != v.end(); ++i) {
        cout << *i << " ";
    }
    cout << endl;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cout << "Usage: ./pagerank <path_to_graph>" << endl;
        exit(-1);
    }

    init();

    Graph g = Graph(argv[1]);
    barrier(); 
    if (rank_me() == 1) {
    cout << "Rank " << rank_me() << endl;
    print_vector(*g.in_edges);
    print_vector(*g.in_offsets);
    print_vector(*g.out_edges);
    print_vector(*g.out_offsets);
    }
    finalize();
}