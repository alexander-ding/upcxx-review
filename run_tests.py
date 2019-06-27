import os
import sys

GRAPHS_UNWEIGHTED = [(10, 100), (100, 1000), (1000, 10000), (10000, 100000),
          (100, 50), (100, 100), (100, 200), (100, 400)]

GRAPHS_WEIGHTED = GRAPHS_UNWEIGHTED

def run_mp(name):
    os.system("./openmp/{}".format(name))

def run_upcxx(name):
    os.system("upcxx-run -n 4 ./upcxx/{}".format(name))

def hello_mp():
    run_mp('hello')

def hello_upcxx():
    run_upcxx('hello')

def random_access_mp():
    print("No MP:\n")
    run_mp('random_access_no_mp')
    print("\nWith MP:\n")
    run_mp('random_access')

def random_access_upcxx():
    print("Without future:\n")
    run_upcxx('random_access')
    print("\nWith future\n")
    run_upcxx('random_access_future')

def pagerank_mp():
    def graph(n, m):
        print("\nGraph: {} nodes; {} edges\n".format(n,m))
        run_mp('pagerank graphs/unweighted/{}_{}.txt'.format(n,m))

    for n, m in GRAPHS_UNWEIGHTED:
        graph(n, m)


def pagerank_upcxx():
    def graph(n, m):
        print("\nGraph: {} nodes; {} edges\n".format(n,m))
        run_upcxx('pagerank graphs/unweighted/{}_{}.txt'.format(n,m))

    for n, m in GRAPHS_UNWEIGHTED:
        graph(n, m)

def bfs_mp():
    def graph(n, m):
        print("\nGraph: {} nodes; {} edges\n".format(n,m))
        run_mp("bfs graphs/unweighted/{}_{}.txt 0".format(n,m))

    for n, m in GRAPHS_UNWEIGHTED:
        graph(n, m)

def bfs_upcxx():
    def graph(n, m):
        print("\nGraph: {} nodes; {} edges\n".format(n,m))
        run_upcxx("bfs graphs/unweighted/{}_{}.txt 0".format(n,m))
    
    for n, m in GRAPHS_UNWEIGHTED:
        graph(n, m)

def bf_mp():
    def graph(n, m):
        if n > 100:
            print("\nGraph: {} nodes; {} edges; all positive\n".format(n, m))
            run_mp("bellman_ford graphs/weighted/{}_{}_pos.txt 0".format(n,m))
        else:
            print("\nGraph: {} nodes; {} edges\n".format(n,m))
            run_mp("bellman_ford graphs/weighted/{}_{}.txt 0".format(n,m))

    for n, m in GRAPHS_WEIGHTED:
        graph(n, m)

def bf_upcxx():
    def graph(n, m):
        if n > 100:
            print("\nGraph: {} nodes; {} edges; all positive\n".format(n,m))
            run_upcxx("bellman_ford graphs/weighted/{}_{}_pos.txt 0".format(n,m))
        else:
            print("\nGraph: {} nodes; {} edges\n".format(n,m))
            run_upcxx("bellman_ford graphs/weighted/{}_{}.txt 0".format(n,m))

    for n, m in GRAPHS_WEIGHTED:
        graph(n, m)
    
def cc_mp():
    def graph(n, m):
        print("\nGraph: {} nodes; {} edges\n".format(n,m))
        run_mp("connected_components graphs/unweighted/{}_{}.txt".format(n,m))

    for n, m in GRAPHS_UNWEIGHTED:
        graph(n, m)

    
def cc_upcxx():
    def graph(n, m):
        print("\nGraph: {} nodes; {} edges\n".format(n,m))
        run_upcxx("connected_components graphs/unweighted/{}_{}.txt".format(n,m))

    for n, m in GRAPHS_UNWEIGHTED:
        graph(n, m)


names = ['hello', 'random_access', 'pagerank', 'bfs', 'bf', "cc"]
mps = [hello_mp, random_access_mp, pagerank_mp, bfs_mp, bf_mp, cc_mp]
upcxxs = [hello_upcxx, random_access_upcxx, pagerank_upcxx, bfs_upcxx, bf_upcxx, cc_upcxx]

def run_task(name, mp, upcxx):
    print("-------------------")
    print("Task {}".format(name))
    print("-------------------")
    print("\n1. OpenMP:\n")
    mp()
    print("\n2. UPC++:\n")
    upcxx()
    print("")

def main():
    if len(sys.argv) == 1:
        print("Usage: python run_tests.py <test_name>|all")
        return

    test = sys.argv[1]

    if test == "all":
        for name, mp, upcxx in zip(names, mps, upcxxs):
           run_task(name, mp, upcxx)
    elif test == "hello":
        run_task(test, hello_mp, hello_upcxx)
    elif test == "pagerank":
        run_task(test, pagerank_mp, pagerank_upcxx)
    elif test == "bfs":
        run_task(test, bfs_mp, bfs_upcxx)
    elif test == "cc":
        run_task(test, cc_mp, cc_upcxx)

if __name__ == "__main__":
    main()