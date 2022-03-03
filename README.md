# Graveyard-of-wikidata

See https://deepnote.com/project/Graveyard-of-wikidata-UT7TctlYRhi13EsYbyVVvQ for online deepnote notebook

See cluster.ipynb for cluster results

## Load files & analysis with PCA & TSNE

Creating file pca.joblib with 27 features and components=10
and tsne.joblib with fit & transform pca results

Load data/pca/pca.noduplicate.joblib and data/tsne/tsne.noduplicate.joblib to load PCA & TSNE results (around 40K queries for noduplicate and 122K queries with duplicates)

(Details in script/pca.py & script/tsne.py)

Load data/sample/status500_Joined.parsed.sample.tsv to load data queries

## Clustering with K-Means

See https://imgur.com/a/G40ezMW to check results of clustering with K-Means and with n clusters (8 to 19 clusters)

