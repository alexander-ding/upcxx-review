#include <iostream>
#include <omp.h>
#include <cstdlib>
#include <chrono>
#include <ctime> 

#include "graph.hpp"

using namespace std;

void print(std::vector<int> const &input)
{ 
    for (int i = 0; i < input.size(); i++) {
        cout << input.at(i) << ' ';
    }
    cout << endl;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cout << "Usage: ./pagerank <path_to_graph>" << endl;
        exit(-1);
    }

    Graph g(argv[1]);
    print(g.out_neighbors(0));
    print(g.out_neighbors(1));
    print(g.in_neighbors(0));
    print(g.in_neighbors(1));
}