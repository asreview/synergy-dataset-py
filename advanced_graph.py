
from pyodss import Dataset
import networkx as nx

# load dataset with referenced_works
d = Dataset("Appenzeller-Herzog_2020")
result = d.to_dict(variables={"title": lambda x: x["title"], "referenced_works": lambda x: x["referenced_works"]})


# make a graph of the dataset with networkx
nodes = [(k, {"label_included":v["label_included"]}) for k, v in result.items()]
edges = [(k, r) for k, v in result.items() if "referenced_works" in v for r in v["referenced_works"] ]

print("Number of nodes", len(nodes))
print("Number of edges", len(edges))

G = nx.Graph()
G.add_nodes_from(nodes)
G.add_edges_from(edges)
G.remove_nodes_from(set(G.nodes) - set([n[0] for n in nodes]))

print("Number of nodes", len(G.nodes))
print("Number of edges", len(G.edges))

nx.write_gexf(G, "Appenzeller-Herzog_2020_network.gexf")

# print(len(set([n[0] for n in nodes]) & set([e[1] for e in edges])))
