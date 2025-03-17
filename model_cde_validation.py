import yaml
import requests
import json
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME
import os
import regex as rx
import pandas as pd

CDE_CODE = "CDECode"
CDE_FULLNAME = "CDEFullName"
DATA_ELEMENT = "DataElement"
VALUE_DOMAIN = "ValueDomain"
PERMISSIBLE_VALUES = "PermissibleValues"
VALUE_MEANING = "ValueMeaning"
CONCEPTS = "Concepts"
VERSION = "version"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'CDE_NCIT'
    os.environ[APP_NAME] = 'CDE_NCIT'
log = get_logger('CDE NCIT')

def model_cde_validation(cde_url_base, props_file, validation_result_file_key, cde_pv_file_key):
    validation_result = {}
    validation_result["cde_property_without_enum"] = {}
    validation_result["enum_property_without_cde"] = {}
    validation_result["pv_in_cde_not_enum"] = {}
    validation_result["pv_in_enum_not_cde"] = {}
    validation_result["cde_property_with_link"] = {}
    validation_result["cde_version_unmatch"] = {}
    cde_pv_dict = {}
    link_property_df_dict = {}


    with open(props_file, 'r') as file:
        props = yaml.safe_load(file)
    for prop in props["PropDefinitions"].keys():
        terms = props["PropDefinitions"][prop].get("Term")
        if terms is None:
            continue
        cde_code = terms[0].get("Code")
        model_cde_version = terms[0].get("Version")
        if cde_code is None:
            continue
        cde_url = cde_url_base + cde_code
        cde_header = {"accept": "application/json"}
        response = requests.get(cde_url, headers=cde_header)
        if response.status_code != 200:
            log.error(f"CDE Request failed with status code {response.status_code}")
            continue
        data = json.loads(response.content)
        if data['status'] == "error":
            log.info(prop)
            continue
        pv_list = data.get(DATA_ELEMENT,{}).get(VALUE_DOMAIN,{}).get(PERMISSIBLE_VALUES)
        cadsr_cde_version = data.get(DATA_ELEMENT,{}).get(VERSION)
        if cadsr_cde_version is not None and model_cde_version is not None:
            if float(model_cde_version) != float(cadsr_cde_version):
                validation_result["cde_version_unmatch"][prop] = {}
                validation_result["cde_version_unmatch"][prop]["model_cde_version"] = model_cde_version
                validation_result["cde_version_unmatch"][prop]["cadsr_cde_version"] = cadsr_cde_version
        if pv_list is None:
            log.error("Can not find the Permissible Values")
            continue
        pv_value_list = []
        enum_list = props["PropDefinitions"][prop].get("Enum")
        for pv in pv_list:
            value = pv.get("value")
            cleaned_value = value.rstrip()
            pv_value_list.append(cleaned_value)
        link_count = 0
        if len(pv_value_list) > 0:
            for permissive_value in pv_value_list:
                if "https" in permissive_value:
                    link_count += 1
        if link_count > 0:
            validation_result["cde_property_with_link"][prop] = pv_value_list
            if enum_list is not None:
                enum_df = pd.DataFrame()
                enum_df[prop] = enum_list
                link_property_df_dict[prop] = enum_df
        if (link_count > 0 or len(pv_value_list) == 0) and enum_list is not None:
            validation_result["enum_property_without_cde"][prop] = enum_list
        if enum_list is None and link_count == 0 and len(pv_value_list) > 0:
            validation_result["cde_property_without_enum"][prop] = pv_value_list
            cde_pv_dict[prop] = pv_value_list
        if enum_list is not None and link_count == 0 and len(pv_value_list) > 0: #has meaningful cde pv list and has enum list
            pv_in_cde_only = []
            pv_in_enum_only = []
            for pve in enum_list:
                if pve not in pv_value_list:
                    pv_in_enum_only.append(pve)
            for pvc in pv_value_list:
                if pvc not in enum_list:
                    pv_in_cde_only.append(pvc)
            if len(pv_in_enum_only) > 0:
                validation_result["pv_in_enum_not_cde"][prop] = pv_in_enum_only
            if len(pv_in_cde_only)>0:
                validation_result["pv_in_cde_not_enum"][prop] = pv_in_cde_only
            if len(pv_in_enum_only) > 0 or len(pv_in_cde_only)>0:
                cde_pv_dict[prop] = pv_value_list
                log.info(f"The property {prop} has different pvs between the model and the cde")


    with open(validation_result_file_key, 'w') as yaml_file:
        yaml.dump(validation_result , yaml_file, default_flow_style=False, width=100000000, sort_keys=False)
    with open(cde_pv_file_key, 'w') as yaml_file:
        yaml.dump(cde_pv_dict , yaml_file, default_flow_style=False, width=100000000, sort_keys=False)

if __name__ == "__main__":
    cde_url = "CDE_URL"
    props_file = "model-props.yml" #model property file
    validation_result_file_key = "tests/model_cde_validation_result.yaml" #output validation result file key
    cde_pv_file_key = "tests/cde_pv.yaml" #output cde pv file key
    model_cde_validation(cde_url, props_file, validation_result_file_key, cde_pv_file_key)