import os
import sys

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
        run_mp('pagerank {}_{}.txt'.format(n,m))

    graph(10, 100)
    graph(100, 1000)
    graph(1000, 10000)
    graph(10000, 100000)

def pagerank_upcxx():
    def graph(n, m):
        print("\nGraph: {} nodes; {} edges\n".format(n,m))
        run_upcxx('pagerank {}_{}.txt'.format(n,m))

    graph(10, 100)
    graph(100, 1000)
    graph(1000, 10000)
    graph(10000, 100000)


names = ['hello', 'random_access', 'pagerank']
mps = [hello_mp, random_access_mp, pagerank_mp]
upcxxs = [hello_upcxx, random_access_upcxx, pagerank_upcxx]

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
    elif test == "pagerank":
        run_task(test, pagerank_mp, pagerank_upcxx)

if __name__ == "__main__":
    main()