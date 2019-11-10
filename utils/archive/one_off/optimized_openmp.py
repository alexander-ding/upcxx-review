import json

with open("output/optimized_openmp_real.json") as f:
    json_data = json.load(f)

tests = ['bf', 'bfs', 'cc', 'pagerank']
graphs = ['facebook_combined', 'twitter_combined', 'com-youtube.ungraph', 'gplus_combined', 'com-orkut.ungraph']

for test in tests:
    print(f"Data for {test}\n")
    for graph in graphs:
        num_nodes = json_data[test][graph]['info']['node_size']
        num_edges = json_data[test][graph]['info']['edge_size']
        print(f"Graph {graph} | Nodes {num_nodes} | Edges {num_edges}\n")
        data = json_data[test][graph]['data']
        for num_nodes, time in data:
            print(f"{num_nodes}\t{time}")
        print()
