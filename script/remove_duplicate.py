#! /usr/bin/env python3

import re
import sys
import pandas as pd
from pandas import concat

input_file = sys.argv[1]
output_file = sys.argv[2]

df = pd.read_csv(input_file, sep='\t')

df_out = df.drop_duplicates(subset="algebraTree")

df_out.to_csv(output_file, sep='\t', index=False)