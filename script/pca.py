
import re
import sys
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from joblib import dump, load
import pandas as pd
import numpy as np
from pandas import concat

input_file = sys.argv[1]
output_file = sys.argv[len(sys.argv) - 2]
features_file = sys.argv[len(sys.argv) - 1]

df = pd.read_csv(input_file, sep='\t')


features = [
    "var_cpt",
    "filter",
    "orderby",
    "limit",
    "select",
    "distinct",
    "join",
    "project",
    "tomultiset",
    "union",
    "modify",
    "bgp",
    "values",
    "groupBy",
    "slice",
    "triples",
    "offset",
    "extend",
    "filter_not_exists",
    "leftJoin",
    "minus",
    "notexist",
    "sample",
    "count",
    "having",
    "complexPathWith",
    "simplePathWith",
    "cycleNumber",
    "isTree",
    "isForest",
    "numberOfNodes",
    "numberOfEdges"
]

df_pca = df[features]
featMat = df_pca.values

#Create object
scaler = StandardScaler()

#Calculate mean and standard deviation
scaler.fit(featMat)

#Transform the values
featMat_scaled = scaler.transform(featMat)

pca_10 = PCA(n_components=10)
featMat_pca_10 = pca_10.fit_transform(featMat_scaled)

print(np.cumsum(pca_10.explained_variance_ratio_ * 100))

dump(featMat_pca_10, output_file)
dump(features,features_file)