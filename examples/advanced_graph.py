# download networkx (pip install networkx) if not available
import networkx as nx

from pyodss import Dataset

dataset_name = "Appenzeller-Herzog_2020"

# load dataset with referenced_works
d = Dataset(dataset_name)
result = d.to_dict(
    variables={
        "title": lambda x: x["title"],
        "referenced_works": lambda x: x["referenced_works"],
    }
)

# make a graph of the dataset with networkx
nodes = [(k, {"label_included": v["label_included"]}) for k, v in result.items()]
edges = [
    (k, r)
    for k, v in result.items()
    if "referenced_works" in v
    for r in v["referenced_works"]
]

print("Number of nodes", len(nodes))
print("Number of edges", len(edges))

G = nx.Graph()
G.add_nodes_from(nodes)
G.add_edges_from(edges)
G.remove_nodes_from(set(G.nodes) - set([n[0] for n in nodes]))

print("Number of nodes", len(G.nodes))
print("Number of edges", len(G.edges))

nx.write_gexf(G, f"{dataset_name}_network.gexf")
