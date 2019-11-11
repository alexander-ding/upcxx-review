import argparse
from pathlib import Path

from utils import add_weights_graph, check_cwd


def main(args):
    input_graph = Path(args.input_graph)
    output_graph = Path(args.output_graph)

    add_weights_graph(input_graph, output_graph)

if __name__ == "__main__":
    check_cwd()

    parser = argparse.ArgumentParser(description="Generates random power-law graphs")
    parser.add_argument('input_graph', type=str)
    parser.add_argument('output_graph', type=str)

    args = parser.parse_args()

    main(args)
