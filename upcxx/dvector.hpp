#ifndef DVECTOR_H
#define DVECTOR_H

#include <vector>
#include <upcxx/upcxx.hpp>

using namespace std;
using namespace upcxx;

template <class T>
class DVector {
    using d_vector = dist_object<vector<T>>;
    
    int next_rank = 0;
    public:
        d_vector local_vec;
        DVector() : local_vec(vector<T>()){};
        future<T> get(const int &index);
        future<> push_back(const T &item);
        future<> set(const int &index, const T &value);

        int size();

    private:
        int get_target_rank(const int &i) const {
            return i % rank_n();
        }
        int get_target_index(const int &i) const {
            return (int)i / rank_n();
        }
};
#include "dvector.cpp"
#endif