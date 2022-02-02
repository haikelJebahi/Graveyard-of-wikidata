#! /usr/bin/env python3

import sys
import csv
import pandas as pd
from urllib.parse import unquote_plus
from pyparsing import ParseException
from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate, pprintAlgebra, traverse
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate
from rdflib.plugins.sparql.operators import Builtin_LANG
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib.term import Variable
import re
from pandas import concat

input_file = sys.argv[1]
output_file = sys.argv[2]

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
    except (ParseException):
        print("ParseException : \n" + raw + "\n")
        return None
    except (RecursionError):
        print("RecursionError : \n" + raw + "\n")
        return None
    except (AttributeError):  # https://github.com/RDFLib/rdflib/issues/813
        print("AttributeError : \n" + raw + "\n")
        return None
    except (TypeError):
        print("TypeError : \n" + raw + "\n")
        return None


df["query"] = df["rawAnonymizedQuery"].apply(raw2parse)

df_nona = df.dropna()


# Node visitor
class NodeVisitor:
    def __init__(self):
        self.var = set()
        self.filter = 0
        self.orderby = 0
        self.limit = 0  # TODO not node       ---------> length(
        self.select = 0
        self.distinct = 0
        self.join = 0
        self.project = 0
        self.tomultiset = 0
        self.union = 0
        self.modify = 0
        self.bgp = 0
        self.values = 0
        self.insertdata = 0
        self.deletedata = 0
        self.groupBy = 0
        self.slice = 0
        self.triples = 0
        self.leftjoin = 0  # AJOUT
        self.offset = 0  # TODO not node       ---------> start(
        self.optional = 0  # TODO
        self.pathWith = 0  # TODO
        self.subquery = 0  # TODO
        self.bind = 0  # TODO
        self.values = 0
        self.notExists = 0  # TODO
        self.minus = 0  # TODO
        self.serviceLang = 0  # TODO
        self.serviceOther = 0  # TODO
        self.sample = 0  # TODO
        self.count = 0  # TODO
        self.groupConcat = 0  # TODO
        self.having = 0  # TODO

    def visite(self, x):
        if isinstance(x, Variable):
            self.var.add(x)

        if isinstance(x, CompValue):
            if x.name == "SelectQuery":
                self.select = self.select + 1
            if x.name == "Filter":
                self.filter = self.filter + 1
            if x.name == "BGP":
                self.bgp = self.bgp + 1

                # add triples to count
                self.triples = self.triples + len(x.triples)
                self.join = self.join + len(x.triples) - 1
            if x.name == "Distinct":
                self.distinct = self.distinct + 1
            if x.name == "Join":
                self.join = self.join + 1
            if x.name == "InsertData":
                self.insertdata = self.insertdata + 1
            if x.name == "DeleteData":
                self.deletedata = self.deletedata + 1
            if x.name == "OrderBy":
                self.orderby = self.orderby + 1
            if x.name == "Slice":
                self.slice = self.slice + 1
                if x.get("start") != "start":
                    self.offset = x.get("start")
                if (x.get("length") != "length"):
                    self.limit = x.get("length")
            if x.name == "Project":
                self.project = self.project + 1
            if x.name == "ToMultiSet":
                self.tomultiset = self.tomultiset + 1
            if x.name == "Union":
                self.union = self.union + 1
            if x.name == "values":
                self.values = self.values + 1
            if x.name == 'Modify':
                self.modify = self.modify + 1
            if x.name == "Group":
                self.groupBy = self.groupBy + 1


def f(query) -> NodeVisitor:
    visitor = NodeVisitor()
    traverse(query.algebra, visitPre=lambda x: visitor.visite(x))
    return visitor


df_nona["visitor"] = df_nona["query"].apply(f)

# Add visitors data to dataframe

df_nona["counter_variable"] = df_nona["visitor"].apply(lambda x: len(x.var))
df_nona["counter_filter"] = df_nona["visitor"].apply(lambda x: x.filter)
df_nona["counter_orderby"] = df_nona["visitor"].apply(lambda x: x.orderby)
df_nona["counter_limit"] = df_nona["visitor"].apply(lambda x: x.limit)
df_nona["counter_select"] = df_nona["visitor"].apply(lambda x: x.select)
df_nona["counter_distinct"] = df_nona["visitor"].apply(lambda x: x.distinct)
df_nona["counter_modify"] = df_nona["visitor"].apply(lambda x: x.modify)
df_nona["counter_tomultiset"] = df_nona["visitor"].apply(lambda x: x.tomultiset)
df_nona["counter_join"] = df_nona["visitor"].apply(lambda x: x.join)
df_nona["counter_bgp"] = df_nona["visitor"].apply(lambda x: x.bgp)
df_nona["counter_insertdata"] = df_nona["visitor"].apply(lambda x: x.insertdata)
df_nona["counter_deletedata"] = df_nona["visitor"].apply(lambda x: x.deletedata)
df_nona["counter_project"] = df_nona["visitor"].apply(lambda x: x.project)
df_nona["counter_union"] = df_nona["visitor"].apply(lambda x: x.union)
df_nona["counter_values"] = df_nona["visitor"].apply(lambda x: x.values)
df_nona["counter_groupby"] = df_nona["visitor"].apply(lambda x: x.groupBy)
df_nona["counter_triples"] = df_nona["visitor"].apply(lambda x: x.triples)
df_nona["counter_offset"] = df_nona["visitor"].apply(lambda x: x.offset)  # TODO
df_nona["counter_slice"] = df_nona["visitor"].apply(lambda x: x.slice)  # TODO

df_nona_serializable = df_nona.drop(columns=["query", "visitor","rawAnonymizedQuery"])
df_nona_serializable.to_csv(output_file, sep='\t', index=False)
