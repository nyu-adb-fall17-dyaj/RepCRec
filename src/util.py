from enum import Enum
class NodeColor(Enum):
    WHITE = 0 # Not visited yet
    GRAY = 1  # Visiting
    BLACK = 2 # Finished visiting

class DetectCycles():
    """
    Performs depth-first search to find all cycles on a directed graph and returns a list of cycles found.

    Authors:
        Da Ying (dy877@nyu.edu)
        Ardi Jusufi (aj2223@nyu.edu)
    """
    def __init__(self):
        self.node_colors = {}
        self.node_parents = {}
        self.chain_of_nodes = [] # list of visited transactions under consideration for cycles
        self.cycles = [] # list of cycles found in graph

    def get_cycles(self, graph):
        """
        Prints all cycles found on graph.
        :param graph: graph to be inspected for cycles
        :return: list of cycles found in graph (in no particular order), if any
        """
        # Initialize all nodes as unvisited:
        for k, v in graph.items():
            self.node_colors[k] = NodeColor.WHITE
            self.node_parents[k] = None

        # Begin visiting nodes using DFS approach:
        for k, v in graph.items():
            if self.node_colors[k] == NodeColor.WHITE:
                self.dfs_visit(graph, k)
        cycles = self.cycles
        self.cycles = []
        return cycles

    def dfs_visit(self, graph, node):
        """
        Performs DFS on graph starting on node. Stores cycles as they are encountered.
        :param graph: graph complete with directed edges where the DFS is performed
        :param node: start node for the DFS
        """
        self.node_colors[node] = NodeColor.GRAY
        self.chain_of_nodes.append(node)
        for neighbor in graph[node]:
            if self.node_colors[neighbor] == NodeColor.WHITE:
                self.node_parents[neighbor] = node
                self.dfs_visit(graph, neighbor)
            # Just encountered a visited node -- this is a cycle:
            elif self.node_colors[neighbor] == NodeColor.GRAY:
                self.cycles.append(self.dfs_get_cycle(neighbor))
        self.node_colors[node] = NodeColor.BLACK
        self.chain_of_nodes = self.chain_of_nodes[:-1]

    def dfs_get_cycle(self, node):
        """
        Extracts and returns the cycle from the visited nodes.
        :param node: node where the cycle starts being inspected
        :return: list of nodes in cycle
        """
        final_chain = [node]
        for n in reversed(self.chain_of_nodes):
            if(n != node):
                final_chain.append(n)
            # Arrived at the beginning of the cycle:
            else:
                break
        return final_chain

class Util:
    @staticmethod
    def get_cycles(graph):
        """
        Returns list of cycles in graph.
        :param graph: graph to be inspected for cycles
        :return: list of cycles in graph, if any
        """
        return DetectCycles().get_cycles(graph)