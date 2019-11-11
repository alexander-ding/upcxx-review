// This code is part of the Problem Based Benchmark Suite (PBBS)
// Copyright (c) 2011 Guy Blelloch and the PBBS team
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights (to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#ifndef A_SEQUENCE_INCLUDED
#define A_SEQUENCE_INCLUDED

#include <iostream>
#include <functional>
#include "utils.hpp"

using namespace std;

namespace sequence {
  template <class intT>
  intT sumFlagsSerial(bool *Fl, intT n) {
    intT sum = 0;
    for (intT i = 0; i < n; i++)
      sum += Fl[i];
    return sum;
  }


  template <class ET, class intT, class PRED> 
  intT filter(ET* In, ET* Out, intT n, PRED p) {
    intT m = 0;
    for (intT i = 0; i < n; i++) {
      if (p(In[i])) {
        Out[m] = In[i];
        m++; 
      }
    }
    return m;
  }

  template <class ET, class intT> 
  ET sum(ET *In, intT n) {
    ET sum = 0;
    for (intT i = 0; i < n; i++) {
      sum += In[i];
    }
    return sum;
  }
}

#endif // _A_SEQUENCE_INCLUDED