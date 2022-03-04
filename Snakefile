# règle qui spécifie les fichiers que l'on veut générer
rule targets:
    input:
      ["data/tsne/tsne.joblib",
      "data/tsne/tsne.noduplicate.joblib"
        ]

rule tsne:
    input:
        "data/pca/pca.joblib"
    output:
        "data/tsne/tsne.joblib"
    shell:
        "/usr/bin/env python3 script/tsne.py {input} {output}"

rule tsne_noduplicate:
    input:
        "data/pca/pca.noduplicate.joblib"
    output:
        "data/tsne/tsne.noduplicate.joblib"
    shell:
        "/usr/bin/env python3 script/tsne.py {input} {output}"

rule pca:
    input:
        "data/sample/status500_Joined.parsed.sample.tsv"
    output:
        ["data/pca/pca.joblib","data/pca/pca_features.joblib"]
    shell:
        "/usr/bin/env python3 script/pca.py {input} {output}"

rule pca_noduplicate:
    input:
        "data/sample/status500_Joined.parsed.noduplicate.sample.tsv"
    output:
        ["data/pca/pca.noduplicate.joblib","data/pca/pca_features.noduplicate.joblib"]
    shell:
        "/usr/bin/env python3 script/pca.py {input} {output}"


rule sample:
    input:
        "data/merged/status500_Joined.parsed.tsv"
    output:
        "data/sample/status500_Joined.parsed.sample.tsv"
    shell:
        # 122832
        "/usr/bin/env python3 script/sample.py {input} {output} 122832"

rule sample_noduplicate:
    input:
        "data/merged/status500_Joined.parsed.noduplicate.tsv"
    output:
        "data/sample/status500_Joined.parsed.noduplicate.sample.tsv"
    shell:
        # 122832
        "/usr/bin/env python3 script/sample.py {input} {output} 122832"

rule remove_duplicate:
    input:
        "data/merged/status500_Joined.parsed.tsv"
    output:
        "data/merged/status500_Joined.parsed.noduplicate.tsv"
    shell:
        "/usr/bin/env python3 script/remove_duplicate.py {input} {output} 122832"


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


