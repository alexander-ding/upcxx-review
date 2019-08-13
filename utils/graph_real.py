import json
import os
import argparse

from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

def graph_upcxx_mp(xs_upcxx, ys_upcxx, xs_mp, ys_mp, test_name, graph_name, node_size, edge_size):
    fig, ax = plt.subplots(figsize=(12, 12), dpi=300)
    title = "{}: {} ({} Nodes | {} Edges)".format(test_name, graph_name, node_size, edge_size)
    ax.set_title(title)
    ax.plot(xs_upcxx, ys_upcxx, label='UPC++', marker='.')
    ax.plot(xs_mp, ys_mp, label='OpenMP', marker='.')
        
    ax.legend()
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Time (s)")
    
    ax.set_xscale('log', basex=2)
    ax.set_yscale('log', basey=10)
    ax.set_xticks(np.array(xs_mp, dtype=np.uint8))
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    
    outpath = output_dir / test_name / "{}.png".format(graph_name )
    plt.savefig(outpath, dpi=300)
    plt.close(fig)

def main():
    with open(input_file_name) as f:
        json_data = json.load(f)

    tests = ['bf', 'bfs', 'cc', 'pagerank']
    graphs = ['facebook_combined', 'twitter_combined', 'gplus_combined', 'com-orkut.ungraph', 'com-youtube.ungraph']
    for test in tests:
        for graph in graphs:
            node_size = json_data[test][graph]['info']['node_size']
            edge_size = json_data[test][graph]['info']['edge_size']
            data = json_data[test][graph]['data']
            xs, ys = list(zip(*data))
            # find second occurence of 1
            dividing_index = [i for i, n in enumerate(xs) if n == 1][1]
            xs_upcxx, ys_upcxx = xs[:dividing_index], ys[:dividing_index]
            xs_mp, ys_mp = xs[dividing_index:], ys[dividing_index:]
            graph_upcxx_mp(xs_upcxx, ys_upcxx, xs_mp, ys_mp, test, graph, node_size, edge_size)

if __name__ == "__main__":
    cws = os.getcwd()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    if os.path.join(cws, "utils") != dir_path:
        raise Exception("run_tests.py must be run from the top-level directory of the repository")
    
    parser = argparse.ArgumentParser(description="Visualize the data")
    parser.add_argument('--input', type=str, default="test_results_real.json", help="The output file generated when the test is completed")
    parser.add_argument("--output_dir", type=str, default="output/", help="The directory to output the data to")

    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    input_file_name = args.input
    main()