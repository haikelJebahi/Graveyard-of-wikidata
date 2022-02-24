#! /usr/bin/env python3

import sys
import csv
import pandas as pd
from urllib.parse import unquote_plus
from pyparsing import ParseException
from rdflib.paths import *
from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate, pprintAlgebra, traverse
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate, Query
from rdflib.plugins.sparql.operators import Builtin_LANG
from rdflib.plugins.sparql.parserutils import *
from rdflib.term import Variable
import re
from pandas import concat

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
        self.__var = set()
        self.var_cpt = 0
        self.filter = 0
        self.orderby = 0
        self.limit = 0
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
        self.offset = 0
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
            was_here = x in self.__var
            if not was_here:
                self.__var.add(x)
                self.var_cpt = self.var_cpt + 1

    def __visite_compvalue__(self, x):
        if isinstance(x, CompValue):
            if x.name == "SelectQuery":
                self.select = self.select + 1

            if x.name == "Filter":
                self.filter = self.filter + 1
                if isinstance(x.expr, Expr):
                    if x.expr.name == "Builtin_NOTEXISTS":
                        self.filter_not_exists = self.filter_not_exists + 1

            if x.name == "BGP":
                self.bgp = self.bgp + 1

                self.triples = self.triples + len(x.triples)
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
                    self.offset = x.get("start")
                if (x.get("length") != "length"):
                    self.limit = x.get("length")
            if x.name == "Project":
                self.project = self.project + 1
            if isinstance(x.A,list):
                if len(x.A)>1:
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


def f(line) -> NodeVisitor:
    if verbose:
        print("timestamp : " + str(line["timestamp"]))
        print("anonymizedQuery : " + str(line["anonymizedQuery"]))
        print("")

    query : Query = line["query"]
    visitor = NodeVisitor()
    traverse(query.algebra, visitPre=lambda x: visitor.visite(x))
    return visitor


df_nona["visitor"] = df_nona.apply(f,axis=1)


# Add visitors data to dataframe
def props(cls):
    return [i for i in cls.__dict__.keys() if i[:1] != '_']


properties = props(NodeVisitor())

for propertyStr in properties:
    df_nona[propertyStr] = df_nona["visitor"].apply(lambda x: getattr(x, propertyStr))

df_nona["complexPathWith"] = df_nona.loc[:,['pathWithStar','pathWithQuestionMark','pathWithPlus']].sum(axis=1)
df_nona["simplePathWith"] = df_nona.loc[:,['pathWithInv','pathWithSequence','pathWithAlternative']].sum(axis=1)

df_nona_serializable = df_nona.drop(columns=["query", "visitor", "rawAnonymizedQuery"])
df_nona_serializable.to_csv(output_file, sep='\t', index=False)
