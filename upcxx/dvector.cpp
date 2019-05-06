#include <vector>

#include "upcxx/upcxx.hpp"
#include "dvector.hpp"

using namespace upcxx;
using namespace std;

template <class T>
future<T> DVector<T>::get(const int &index)  {
    return rpc(get_target_rank(index),
               [this](d_vector &local_vec, int index) -> T {
                   return (*local_vec)[index];
               }, local_vec, get_target_index(index));
}

template <class T>
future<> DVector<T>::set(const int &index, const T &value) {
    return rpc(get_target_rank(index), 
               [this](d_vector &local_vec, int index, const T &value) {
                   (*local_vec)[index] = value;
               }, local_vec, get_target_index(index), value);
}

template <class T>
future<> DVector<T>::push_back(const T &item)  {
    return rpc(get_target_rank(size()), 
               [this](d_vector &local_vec, T item) {
                   local_vec->push_back(item);
               }, local_vec, item);
}

template <class T>
int DVector<T>::size() {
    int total = 0;
    for (int i = 0; i < rank_n(); i++) {
        total += local_vec.fetch(i).wait().size();
    }
    return total;
}