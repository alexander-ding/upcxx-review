#include "upcxx/upcxx.hpp"
using namespace upcxx;
using namespace std;

int main() {
    init();
    int array_size = 10;
    dist_object<int*> test_array(new int[array_size]);
    int rank_start = (rank_me() * array_size) / rank_n(); 
    int rank_step = array_size / rank_n();
    cout << rank_start << " " << rank_step << endl;
    if (rank_me() == 1)
    for (int i = rank_start; i < rank_start+rank_step; i++) {
        (*test_array)[i] = i;
    }
    broadcast((*test_array)+rank_start, rank_step, 1).wait();
    barrier();
    for (int i = 0; i < array_size; i++) 
        assert((*test_array)[i] == i);
}