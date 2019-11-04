OPENMP_FLAGS = -fopenmp

EXTRA_FLAGS = -g -std=c++11

PROGRAMS = \
  bellman_ford \
  bfs \
  connected_components \
  hello \
  pagerank \
  random_access \
  graph_scan

all: $(PROGRAMS)

# For any example
%: %.cpp $(wildcard *.h) $(wildcard *.hpp)
	$(CXX) $@.cpp $(OPENMP_FLAGS) $(EXTRA_FLAGS) -o $@

clean:
	rm -f $(PROGRAMS)

.PHONY: clean all