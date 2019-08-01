#include <iostream>
#include <chrono>
#include <ctime>
#include "upcxx/upcxx.hpp"

using namespace std;
using namespace upcxx;

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cout << "Usage: ./random_access <array_size>" << endl;
        return -1;
    }
    init();
    const int N_array_prelim = atoi(argv[1]);
    const int N_array = int(N_array_prelim / rank_n()) * rank_n(); // take a divisble number
    const int N_test = 1000000;

    int N_array_local = int(N_array / rank_n());
    int N_test_local = int(N_test / rank_n());

    dist_object<global_ptr<int>> A_dist(new_array<int>(N_array_local));
    dist_object<global_ptr<int>> B_dist(new_array<int>(N_array_local));
    dist_object<global_ptr<int>> R_dist(new_array<int>(N_test_local));

    int *A = A_dist->local();
    int *B = B_dist->local();
    int *R = R_dist->local();

    for (int i = 0; i < N_array_local; i++) {
        A[i] = rand() % 10; // fill in some arbitrary values
    }
    for (int i = 0; i < N_test_local; i++) {
        R[i] = rand() % N_array; // fill in some arbitrary values
    }
    barrier();

    auto time_before = chrono::system_clock::now();
    int n_panel, i_panel;
    global_ptr<int> base_panel = nullptr;
    for (int i = 0; i < N_test_local; i++) {
        n_panel = int(R[i] / N_array_local);
        i_panel = R[i] % N_array_local;
        base_panel = A_dist.fetch(n_panel).wait();
        B[i%N_array_local] = rget(base_panel+i_panel).wait();
    }

    barrier();

    auto time_after_read = chrono::system_clock::now();
    std::chrono::duration<double> read_time = time_after_read - time_before;

    for (int i = 0; i < N_test_local; i++) {
        n_panel = int(R[i] / N_array_local);
        i_panel = R[i] % N_array_local;
        base_panel = B_dist.fetch(n_panel).wait();
        rput(A[i%N_array_local], base_panel+i_panel).wait();
    }
    
    barrier();

    auto time_after_write = chrono::system_clock::now();
    std::chrono::duration<double> write_time = time_after_write - time_after_read;

    if (rank_me() == 0) {
        std::cout << read_time.count() << std::endl;
        std::cout << write_time.count() << std::endl;
    }
    

    finalize();
}