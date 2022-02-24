import sys
from joblib import dump, load
from sklearn.manifold import TSNE

input_file = sys.argv[1]
output_file = sys.argv[len(sys.argv) - 1]

pca = load(input_file)
tsne = TSNE(n_components=2, verbose=1, random_state=42)
tsne_full = tsne.fit_transform(pca)

dump(tsne_full, output_file)
