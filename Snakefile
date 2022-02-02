# règle qui spécifie les fichiers que l'on veut générer
rule targets:
    input:
      "data/merged/status500_Joined.parsed.tsv"


def merge_inputs(wildcards):
    files = expand("data/parsed/{file}.parsed.tsv", file=[
        "I1_status500_Joined",
        "I2_status500_Joined",
        "I3_status500_Joined",
        "I4_status500_Joined",
        "I5_status500_Joined",
        "I6_status500_Joined",
        "I7_status500_Joined",
    ])
    return files

rule merge:
    input:
        merge_inputs
    output:
        "data/merged/status500_Joined.parsed.tsv"
    shell:
        "/usr/bin/env python3 script/merge.py {input} {output}"

rule parse:
    input:
        "data/raw/{base}.tsv"
    output:
        "data/parsed/{base}.parsed.tsv"
    shell:
        "/usr/bin/env python3 script/parse.py {input} {output}"