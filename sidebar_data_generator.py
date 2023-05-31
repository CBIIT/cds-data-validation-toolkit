import requests
import yaml
import pandas as pd

query = '''{
    searchSubjects{
        subjectCountByStudy {
            group
            subjects}
        subjectCountByExperimentalStrategy {
            group
            subjects}
        subjectCountByAccess {
            group
            subjects}
        subjectCountByGender {
            group
            subjects}
        subjectCountByIsTumor {
            group
            subjects}
        subjectCountByAnalyteType {
            group
            subjects}
        subjectCountByFileType {
            group
            subjects}
        subjectCountByDiseaseSite {
            group
            subjects}
        subjectCountByLibraryStrategy {
            group
            subjects}
        subjectCountByLibrarySource {
            group
            subjects}
        subjectCountByLibrarySelection {
            group
            subjects}
        subjectCountByLibraryLayout {
            group
            subjects}
        subjectCountByPlatform {
            group
            subjects}
        subjectCountByInstrumentModel {
            group
            subjects}
        subjectCountByReferenceGenomeAssembly {
            group
            subjects}
        subjectCountByPrimaryDiagnosis {
            group
            subjects}
        subjectCountByPhsAccession {
            group
            subjects}
        subjectCountByStudyDataType {
            group
            subjects}
        subjectCountByAcl {
            group
            subjects}
}}'''
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
