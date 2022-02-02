#! /usr/bin/env python3

import re
import sys

import pandas as pd
from pandas import concat

input_file = sys.argv[1: len(sys.argv) - 1: 1]
output_file = sys.argv[len(sys.argv) - 1]

df = pd.DataFrame()
for nom in input_file:  # test file 2
    tmp_df = pd.read_csv(nom, sep='\t')
    tmp_df["file_path"] = nom
    interval = re.findall(r"\d+", nom)[0]
    tmp_df["interval"] = interval
    df = concat([df, tmp_df])

df.to_csv(output_file, sep='\t', index=False)
