import pandas as pd
import yaml
#import numpy as np

excel_file_path = "cde.xlsx"
yaml_path = "cds-model-props.yml"
df = pd.read_excel(excel_file_path)
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)


with open(yaml_path) as f:
        cds_model_props = yaml.load(f, Loader = yaml.FullLoader)

for prop in cds_model_props["PropDefinitions"]:
    if prop in list(df['Field']):
        line_numbers = df[df['Field'] == prop].index.tolist()
        line_number = line_numbers[0]
        origin = df.loc[line_number, "CDE Source"]
        code = df.loc[line_number, "CDE (primary)"]
        value = df.loc[line_number, "caDSR value"]
        version = df.loc[line_number, "Version"]
        term_dict = {}
        term_dict["Origin"] = "caDSR"
        if not pd.isna(origin):
            term_dict["Original Source"] = origin
        if not pd.isna(code):
            term_dict["Code"] = code
        if not pd.isna(value):
            term_dict["Value"] = value
        if not pd.isna(version):
            term_dict["Version"] = "{:.2f}".format(version)
        else:
            term_dict["Version"] = "{:.2f}".format(1)
        if len(term_dict) > 0:
            print(term_dict)
            if "Term" not in cds_model_props["PropDefinitions"][prop].keys():
                cds_model_props["PropDefinitions"][prop]["Term"] = []
            cds_model_props["PropDefinitions"][prop]["Term"].append(term_dict)

with open("new_cds-model-props.yml", 'w') as yaml_file:
    yaml.dump(cds_model_props, yaml_file, default_flow_style=False, width=100000000)