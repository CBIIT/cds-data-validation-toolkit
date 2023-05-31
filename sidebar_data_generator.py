import requests
import yaml
import pandas as pd

query = '''{\r\n
    searchSubjects{\r\n
        subjectCountByStudy {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByExperimentalStrategy {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByAccess {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByGender {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByIsTumor {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByAnalyteType {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByFileType {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByDiseaseSite {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByLibraryStrategy {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByLibrarySource {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByLibrarySelection {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByLibraryLayout {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByPlatform {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByInstrumentModel {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByReferenceGenomeAssembly {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByPrimaryDiagnosis {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByPhsAccession {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByStudyDataType {\r\n
            group\r\n
            subjects\r\n}\r\n
        subjectCountByAcl {\r\n
            group\r\n
            subjects\r\n}\r\n
}\r\n}'''
url = ''
headers = {'Content-Type': 'application/json'}
body = {'query': query}
response = requests.post(url, headers=headers, json=body, verify=False)
sidebar = response.json()
sidebar_dict = {}

for filter_key in sidebar["data"]["searchSubjects"].keys():
    f_list = []
    f_df = pd.DataFrame()
    for i in sidebar["data"]["searchSubjects"][filter_key]:
        f_list.append(i['group'])
    if len(f_list) == 0:
        print(filter_key)
    sorted_f_list = sorted(f_list, key=lambda x: x.lower())
    f_df['properties'] = sorted_f_list
    sidebar_dict[filter_key] = f_df



# saving the dataframe



writer=pd.ExcelWriter('sidebar.xlsx', engine='xlsxwriter', engine_kwargs={'options':{'strings_to_urls': False}})
for key in sidebar_dict.keys():
    sheet_name_new = key.replace("subjectCountBy", "")
    sidebar_dict[key].to_excel(writer,sheet_name=sheet_name_new, index=False)

writer.save()
