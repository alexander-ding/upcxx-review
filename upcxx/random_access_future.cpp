#include <iostream>
#include <chrono>
#include <ctime>
#include "upcxx/upcxx.hpp"

using namespace std;
using namespace upcxx;

int main(int argc, char *argv[]) {
    init();
    const int N_array = 100000;
    const int N_test = 1000000;
    const int PROGRESS_EVERY_N = 200;

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

    future<> fut_all_read = upcxx::make_future();
    for (int i = 0; i < N_test_local; i++) {
        n_panel = int(R[i] / N_array_local);
        i_panel = R[i] % N_array_local;
        future<> fut = A_dist.fetch(n_panel).then(
            [&](global_ptr<int> base_panel) {
                future<> fut_2 = rget(base_panel+i_panel).then(
                    [&](int value) {
                        B[i%N_array_local] = value;
                    }
                );
                fut_all_read = when_all(fut_all_read, fut_2);
            }
        );

        if (i % PROGRESS_EVERY_N == 0) progress();
    }
    progress();
    fut_all_read.wait();
    barrier();

    auto time_after_read = chrono::system_clock::now();
    std::chrono::duration<double> read_time = time_after_read - time_before;

    future<> fut_all_write = upcxx::make_future();
    for (int i = 0; i < N_test_local; i++) {
        n_panel = int(R[i] / N_array_local);
        i_panel = R[i] % N_array_local;
        future<> fut = B_dist.fetch(n_panel).then(
            [&](global_ptr<int> base_panel) {
                future<> fut_2 = rput(A[i%N_array_local], base_panel+i_panel);
                fut_all_write = when_all(fut_all_write, fut_2);
            });
        fut_all_write = when_all(fut_all_write, fut);
        if (i % PROGRESS_EVERY_N == 0) progress();
    }
    progress();
    fut_all_write.wait();
    
    
    barrier();

    auto time_after_write = chrono::system_clock::now();
    std::chrono::duration<double> write_time = time_after_write - time_after_read;

    if (rank_me() == 0) {
        std::cout << "Read time: " << read_time.count() << "s" << std::endl;
        std::cout << "Write time: " << write_time.count() << "s" << std::endl;
    }
    

    finalize();
}