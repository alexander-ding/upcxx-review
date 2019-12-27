# To use this makefile, set the UPCXX_INSTALL variable to the upcxx install directory, e.g.
# make UPCXX_INSTALL=<myinstalldir> hello-world
# or (for bash)
# export UPCXX_INSTALL=<myinstalldir>; make hello-world

ifeq ($(wildcard $(UPCXX_INSTALL)/bin/upcxx-meta),)
$(error Please set UPCXX_INSTALL=/path/to/upcxx/install)
endif

UPCXX_THREADMODE ?= seq
ENV = env UPCXX_THREADMODE=$(UPCXX_THREADMODE)
CXX = $(UPCXX_INSTALL)/bin/upcxx

PTHREAD_FLAGS = -pthread
OPENMP_FLAGS = -fopenmp
export UPCXX_CODEMODE = O3

# Programs to build, assuming each has a corresponding *.cpp file
PROGRAMS = \
  tools/bfs

all: $(PROGRAMS)

# The rule for building any example.
tools/%: tools/%.cpp $(wildcard core/*.hpp)
	$(CXX) $@.cpp -Icore $(EXTRA_FLAGS) -o $@

clean:
	rm -f $(PROGRAMS)

.PHONY: clean all