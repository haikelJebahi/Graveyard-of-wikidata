import pandas as pd
from urllib.parse import unquote_plus

for i in range(1,8) :
    nom = '../BadRequeteCrypt/I'+str(i)+'_status500_Joined.tsv'
    cible = 'I'+str(i)+'_status500_Joined.plain.csv'
    df = pd.read_csv(nom, sep='\t')
    df["anonymizedQuery"] = df["anonymizedQuery"].apply(lambda x: unquote_plus(x).replace("\n",""))
    df.to_csv(cible, sep='\t', index = False)
