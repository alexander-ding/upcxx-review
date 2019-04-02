
#include <iostream>
#include <omp.h>

int main(int argc, char *argv[]) {
    int thread_id;
    #pragma omp parallel private(thread_id)
    {
        thread_id = omp_get_thread_num();
        std::cout << "Hello world from thread" << thread_id << std::endl;

    #pragma omp barrier
        if (thread_id == 0) {
            std::cout << "There are " << omp_get_num_threads() << " threads" << std::endl;
        }
    }
}