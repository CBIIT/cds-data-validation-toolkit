import requests
import yaml
import pandas as pd

query = '''{\r\n    searchSubjects{\r\n        subjectCountByStudy {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByExperimentalStrategy {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByAccess {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByGender {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByIsTumor {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByAnalyteType {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByFileType {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByDiseaseSite {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByLibraryStrategy {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByLibrarySource {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByLibrarySelection {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByLibraryLayout {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByPlatform {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByInstrumentModel {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByReferenceGenomeAssembly {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByPrimaryDiagnosis {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByPhsAccession {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByStudyDataType {\r\n  group\r\n  subjects\r\n}\r\nsubjectCountByAcl {\r\n  group\r\n  subjects\r\n}\r\n    }\r\n}'''
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