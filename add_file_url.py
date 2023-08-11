import boto3
import os
import pandas as pd


s3 = boto3.client('s3',
                  aws_access_key_id='',
                  aws_secret_access_key='')

s3_list = []
file_sep = "/"
#put subfolder directory in the list
subfolder_directory_list = ["subfolder1/","subfolder2/", "subfolder3/"]
#the bucket where the file is
bucket_name = "bucket_name"
#the excel file name
file = "file_name.xlsx"
for subfolder_directory in subfolder_directory_list:
    for i in s3.list_objects(Bucket = bucket_name, Prefix = subfolder_directory)['Contents']:
        if not i["Key"].endswith("/"):
            s3_list.append(i["Key"])

df = pd.DataFrame()
df = pd.read_excel(io = file, sheet_name='Metadata', engine = "openpyxl", keep_default_na = False)

for n in range(0, len(df)):
    for i in s3_list:
        i_list = i.split(file_sep)
        file_name = i_list[len(i_list) - 1]
        if file_name == df.iloc[n]["file_name"]:
            df["file_url_in_cds"].iloc[n] = "s3://" + bucket_name + file_sep + i


writer=pd.ExcelWriter(file, engine='openpyxl', mode="a",  if_sheet_exists="replace")
df.to_excel(writer, sheet_name = 'Metadata', index=False)
writer.close()