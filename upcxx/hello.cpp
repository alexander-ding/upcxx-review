#include <iostream>
#include "upcxx/upcxx.hpp"

using namespace std;

int main(int argc, char *argv[]) {
    upcxx::init();

    cout << "Hello world from thread " << upcxx::rank_me() << endl;

    if (upcxx::rank_me() == 0) {
        cout << "There are " << upcxx::rank_n() << " threads" << endl;
    }

    upcxx::finalize();
}