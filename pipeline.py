import csv
from collections import deque
import itertools


class DAG:

    """
    DAG stands for 'directed acyclic algorithm'. That's basically what this class does.
    Creating an algorithm to later run pipeline tasks in correct order.
    
    """
    def __init__(self):
        """
        self.graph dict is used to store all the nodes of the directed graph
        Parameters: none
        """
        self.graph = {}

    def in_degrees(self):
        """
        Counts the number of in-degrees of nodes in the graph i.e. counts of edges pointing to a node.
        Parameters: none
        Return values: dict, in-degrees of all the nodes in the graph
        """
        in_degrees = {}
        for node in self.graph:
            if node not in in_degrees:
                in_degrees[node] = 0
            for pointed in self.graph[node]:
                if pointed not in in_degrees:
                    in_degrees[pointed] = 0
                in_degrees[pointed] += 1
        return in_degrees

    def sort(self):
        """
        Sorts the nodes based of their in_degrees
        Parameters: none
        Return values: list, nodes in sorted order
        """
        in_degrees = self.in_degrees()
        to_visit = deque()
        for node in self.graph:
            if in_degrees[node] == 0:
                to_visit.append(node)

        searched = []
        while to_visit:
            node = to_visit.popleft()
            for pointer in self.graph[node]:
                in_degrees[pointer] -= 1
                if in_degrees[pointer] == 0:
                    to_visit.append(pointer)
            searched.append(node)
        return searched

    def add(self, node, to=None):

        """
        Adds node and it's dependencies to self.graph dict
        Parameters: 
            node: int, node to be added
            to: int, node which the node points to

        Raises:
            Exception if a node causes a cycle
        """

        if node not in self.graph:
            self.graph[node] = []
        if to:
            if to not in self.graph:
                self.graph[to] = []
            self.graph[node].append(to)
        dependencies = self.sort()
        if len(dependencies) > len(self.graph):
            raise Exception


class Pipeline:
    """
    Class for the task pipeline.

    """
    def __init__(self):
        """
        self.tasks is a DAG object for the pipeline tasks in correct order
        """
        self.tasks = DAG()

    def task(self, depends_on=None):
        """
        Adding a task and it's dependencies to the DAG object self.tasks
        Parameters: int, name of the task the new task depends on
        Return values: inner function to continue the pipeline
        """
        def inner(f):
            self.tasks.add(f)
            if depends_on:
                self.tasks.add(depends_on, f)
            return f
        return inner

    def run(self):
        """
        Runs DAG.sort method to sort the tasks, then runs pipeline.task methods in order
        Parameters: none
        Return values: dict, completed tasks and inner return value of task function

        """
        scheduled = self.tasks.sort()
        completed = {}

        for task in scheduled:
            for node, values in self.tasks.graph.items():
                if task in values:
                    completed[task] = task(completed[node])
            if task not in completed:
                completed[task] = task()
        return completed


def build_csv(lines, header=None, file=None):
    """
    Helper function to build a CSV file
    Parameters: 
        list, lines to be written
        list, names of headers
        file, CSV file object to write in
    Return values: written file
    """
    
    if header:
        lines = itertools.chain([header], lines)
    writer = csv.writer(file, delimiter=',')
    writer.writerows(lines)
    file.seek(0)
    return file