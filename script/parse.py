#! /usr/bin/env python3
import base64
import json
import sys
import csv
import pandas as pd
from urllib.parse import unquote_plus, quote_plus

from joblib import dump
from pyparsing import ParseException
from rdflib.paths import *
from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate, pprintAlgebra, traverse
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate, Query
from rdflib.plugins.sparql.operators import Builtin_LANG
from rdflib.plugins.sparql.parserutils import *
from rdflib.term import Variable
import re
import io
from contextlib import redirect_stdout
import networkx as nx
import hashlib

input_file = sys.argv[1]
output_file = sys.argv[2]

verbose: bool = True

df = pd.read_csv(input_file, sep='\t')


# Decode
def encoded2raw(encoded):
    str = unquote_plus(encoded)
    str = re.sub(r"> *\*", ">*", str)
    str = re.sub(r"> *\+", ">+", str)
    str = re.sub(r"> *\?([^v])", r">?\1", str)
    return str


df["rawAnonymizedQuery"] = df["anonymizedQuery"].apply(encoded2raw)


# Parse
def raw2parse(raw):
    try:
        parsed = parseQuery(raw)
        translated = translateQuery(parsed)
        return translated
    except Exception as e:
        if verbose:
            print(type(e))
            print(raw)
            print()


df["query"] = df["rawAnonymizedQuery"].apply(raw2parse)

df_nona = df.dropna()


# Node visitor
class NodeVisitor:
    def __init__(self):
        self._var = set()
        self._triplesSet = []
        self.var_cpt = 0
        self.filter_regex = 0
        self.filter_relational_expression = 0
        self.orderby = 0
        self._limit = 0
        self.select = 0
        self.distinct = 0
        self.join = 0
        self.project = 0
        self.tomultiset = 0  # subquerry
        self.union = 0
        self.modify = 0
        self.bgp = 0
        self.values = 0
        self.groupBy = 0
        self.slice = 0
        self.triples = 0
        self._offset = 0
        self.extend = 0
        self.filter_not_exists = 0
        self.leftJoin = 0
        self.minus = 0
        self.notexist = 0
        self.sample = 0
        self.count = 0
        self.having = 0

        self.pathWithStar = 0
        self.pathWithPlus = 0
        self.pathWithQuestionMark = 0
        self.pathWithInv = 0
        self.pathWithSequence = 0
        self.pathWithAlternative = 0

    def visite(self, node):
        if isinstance(node, Expr):
            self.__visite_Expr__(node)

        if isinstance(node, Variable):
            self.__visite_variable__(node)

        if isinstance(node, CompValue):
            self.__visite_compvalue__(node)

        if isinstance(node, MulPath):
            self.__visite_mulpath__(node)

        if isinstance(node, InvPath):
            self.__visite_invpath__(node)

        if isinstance(node, SequencePath):
            self.__visite_sequencepath__(node)

        if isinstance(node, AlternativePath):
            self.__visite_alternativepath__(node)

    def __visite_variable__(self, x):
        if isinstance(x, Variable):
            was_here = x in self._var
            if not was_here:
                self._var.add(x)
                self.var_cpt = self.var_cpt + 1

    def __visite_compvalue__(self, x):
        if isinstance(x, CompValue):
            if x.name == "ServiceGraphPattern":
                if x.graph.part != None and x.graph.part[0].triples != None:
                    for triple in x.graph.part[0].triples:
                        self._triplesSet.append(triple)

                if x.graph.where is not None:
                    for part in x.graph.where.part:
                        if part.triples != None:
                            for triple in part.triples:
                                self._triplesSet.append(triple)

            if x.name == "SelectQuery":
                self.select = self.select + 1

            if x.name == "BGP":
                self.bgp = self.bgp + 1

                self.triples = self.triples + len(x.triples)

                for t in x.triples:
                    self._triplesSet.append(
                        [str(t[0]), str(t[1]), str(t[2])]
                    )

                # add triples to count
                if len(x.triples) >= 2:
                    self.join = self.join + len(x.triples) - 1
            if x.name == "Distinct":
                self.distinct = self.distinct + 1
            if x.name == "Join":
                self.join = self.join + 1
            if x.name == "Minus":
                self.minus = self.minus + 1
            if x.name == "LeftJoin":
                self.leftJoin = self.leftJoin + 1
            if x.name == "OrderBy":
                self.orderby = self.orderby + 1
            if x.name == "Builtin_NOTEXISTS":
                self.notexist = self.notexist + 1
            if x.name == "Slice":
                self.slice = self.slice + 1
                if x.get("start") != "start":
                    self._offset = x.get("start")
                if (x.get("length") != "length"):
                    self._limit = x.get("length")
            if x.name == "Project":
                self.project = self.project + 1
            if isinstance(x.A, list):
                if len(x.A) > 1:
                    if "Sample" in str(x.A[1]):
                        self.sample = self.sample + 1
                    if "Count" in str(x.A[1]):
                        self.count = self.count + 1

            if x.name == "ToMultiSet":
                self.tomultiset = self.tomultiset + 1
            if x.name == "Union":
                self.union = self.union + 1
            if x.name == "values":
                self.values = self.values + 1
            if x.name == "Group":
                self.groupBy = self.groupBy + 1
            if x.name == "Extend":
                self.extend = self.extend + 1
            if x.name == "RelationalExpression":
                self.having = self.having + 1

    def __visite_mulpath__(self, x):  # * + ?
        if isinstance(x, MulPath):
            if x.mod == "*":
                self.pathWithStar = self.pathWithStar + 1

            if x.mod == "+":
                self.pathWithPlus = self.pathWithPlus + 1

            if x.mod == "?":
                self.pathWithQuestionMark = self.pathWithQuestionMark + 1

    def __visite_invpath__(self, x):  # ^
        if isinstance(x, InvPath):
            # we need to go deeper, example :
            # ^<http://www.wikidata.org/prop/direct/P279>*
            self.visite(x.arg)
            self.pathWithInv = self.pathWithInv + 1

    def __visite_sequencepath__(self, x):  # /
        if isinstance(x, SequencePath):
            # we need to go deeper, example :
            # <http://www.wikidata.org/prop/direct/P31> / <http://www.wikidata.org/prop/direct/P279>*
            for child in x.args:
                self.visite(child)

            self.pathWithSequence = self.pathWithSequence + 1

    def __visite_alternativepath__(self, x):  # |
        if isinstance(x, AlternativePath):
            # we need to go deeper, example :
            # <http://www.wikidata.org/prop/direct/P31> | <http://www.wikidata.org/prop/direct/P279>*
            for child in x.args:
                self.visite(child)

            self.pathWithAlternative = self.pathWithAlternative + 1

    def __visite_Expr__(self, x):
        if isinstance(x, Expr):
            if x.name == "Builtin_NOTEXISTS":
                self.filter_not_exists = self.filter_not_exists + 1

            if x.name == "Builtin_REGEX":
                self.filter_regex = self.filter_regex + 1

            if x.name == "RelationalExpression":
                self.filter_relational_expression = self.filter_relational_expression + 1





def f(line) -> NodeVisitor:
    if verbose:
        print("timestamp : " + str(line["timestamp"]))
        print("anonymizedQuery : " + str(line["anonymizedQuery"]))
        print("")

    query: Query = line["query"]
    visitor = NodeVisitor()
    traverse(query.algebra, visitPre=lambda x: visitor.visite(x))
    return visitor


df_nona["visitor"] = df_nona.apply(f, axis=1)


# Add visitors data to dataframe
def props(cls):
    return [i for i in cls.__dict__.keys() if i[:1] != '_']


properties = props(NodeVisitor())

for propertyStr in properties:
    print(propertyStr)
    df_nona[propertyStr] = df_nona["visitor"].apply(lambda x: getattr(x, propertyStr))

df_nona["triplesSet"] = df_nona["visitor"].apply(
    lambda visitor: json.dumps([(str(t[0]), str(t[1]), str(t[2])) for t in visitor._triplesSet])
)

df_nona["limit"] = df_nona["visitor"].apply(
    lambda visitor: (0 if visitor._limit == 0 else 1)
)
df_nona["limitValue"] = df_nona["visitor"].apply(
    lambda visitor: visitor._limit
)
df_nona["offset"] = df_nona["visitor"].apply(
    lambda visitor: 0 if visitor._offset == 0 else 1
)
df_nona["offsetValue"] = df_nona["visitor"].apply(
    lambda visitor: visitor._offset
)
df_nona["complexPathWith"] = df_nona.loc[:, ['pathWithStar', 'pathWithQuestionMark', 'pathWithPlus']].sum(axis=1)
df_nona["simplePathWith"] = df_nona.loc[:, ['pathWithInv', 'pathWithSequence', 'pathWithAlternative']].sum(axis=1)
df_nona["simplePathWith"] = df_nona.loc[:, ['pathWithInv', 'pathWithSequence', 'pathWithAlternative']].sum(axis=1)
df_nona["algebraTree"] = df_nona["query"].apply(lambda query: str(query.algebra))
df_nona["algebraTreeMD5"] = df_nona["algebraTree"].apply(
    lambda algebraTree : hashlib.md5(algebraTree.encode()).hexdigest()
)
df_nona["filter"] = df_nona["filter_regex"] + df_nona["filter_relational_expression"]

# Construction des graphes
def func_to_graphe(v: NodeVisitor) -> nx.DiGraph:
    edges = dict()
    for triple in v._triplesSet:
        edges[triple[0], triple[2]] = triple[1]

    graph = nx.DiGraph()
    graph.add_edges_from(edges)
    return graph


df_nona["graph"] = df_nona["visitor"].apply(func_to_graphe)


# Ajouts des features

def func_is_tree(graph):
    if len(graph) == 0:
        return 0
    else:
        if nx.is_tree(graph):
            return 1
        else:
            return 0


def func_is_forest(graph):
    if len(graph) == 0:
        return 0
    else:
        if nx.is_forest(graph):
            return 1
        else:
            return 0


def dump2base64(graph):
    bytes = io.BytesIO()
    dump(graph, bytes)
    graphDump = base64.b64encode(bytes.getvalue()).decode()
    return str(graphDump)

def func_averageDegree(graph):
    if len(graph) == 0:
        return 0

    return sum([val for (node, val) in graph.degree]) / len(graph.degree)


df_nona["cycleNumber"] = df_nona["graph"].apply(lambda graph: len(list(nx.simple_cycles(graph))))
df_nona["graphDump"] = df_nona["graph"].apply(dump2base64)
df_nona["isForest"] = df_nona["graph"].apply(func_is_forest)
df_nona["numberOfNodes"] = df_nona["graph"].apply(lambda graph: graph.number_of_nodes())
df_nona["numberOfEdges"] = df_nona["graph"].apply(lambda graph: graph.number_of_edges())
df_nona["treewidth"] = df_nona["graph"].apply(
    lambda graph: nx.algorithms.approximation.treewidth_min_fill_in(graph.to_undirected())[0])
df_nona["isTree"] = df_nona["graph"].apply(func_is_tree)
df_nona["averageDegree"] = df_nona["graph"].apply(func_averageDegree)
df_nona["maxCliqueWeigth"] = df_nona["graph"].apply(
    lambda graph: nx.max_weight_clique(graph.to_undirected(), weight=None)[1]
)

# Output
df_nona_serializable = df_nona.drop(columns=[
    "query", "visitor", "rawAnonymizedQuery"
    #,"graph"
    #,"algebraTree"
    ])
df_nona_serializable.to_csv(output_file, sep='\t', index=False)

