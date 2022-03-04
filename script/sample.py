import pandas as pd
import sys

input_file = sys.argv[1]
output_file = sys.argv[2]
qtt = int(sys.argv[3])


df = pd.read_csv(input_file, sep='\t')

if len(df) < qtt:
    qtt = len(df)


df_sample = df.sample(n=qtt,random_state=42)
df_sample.to_csv(output_file, sep='\t', index=False)