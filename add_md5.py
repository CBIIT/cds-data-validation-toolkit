import boto3
from bento.common.utils import get_stream_md5
import pandas as pd
import hashlib

s3_client = boto3.client('s3', aws_access_key_id='', aws_secret_access_key='') # need to add the access key and the aws secret access key

file = "tests/test.xlsx" #the excel file name, the excel file must have file_url_in_cds
df = pd.DataFrame()
df = pd.read_excel(io = file, sheet_name='Metadata', engine = "openpyxl", keep_default_na = False)

for index, rows in df.iterrows():
    print("Start calculating md5, will take lots of time streaming the file")
    file_url = df.loc[index, 'file_url_in_cds']
    file_url_list = file_url.split('/')
    s3_bucket = file_url_list[2]
    s3_file_key = ""
    for i in range(3, len(file_url_list)):
        if i != len(file_url_list) - 1:
            s3_file_key = s3_file_key + file_url_list[i] + "/"
        else:
            s3_file_key = s3_file_key + file_url_list[i]
    s3_file = s3_client.get_object(Bucket=s3_bucket, Key=s3_file_key)
    s3_file_content = s3_file['Body']
    s3_hash = get_stream_md5(s3_file_content)
    print(f"The md5sum is {s3_hash}")
    df.loc[index, 'md5sum'] = s3_hash

writer=pd.ExcelWriter(file, engine='openpyxl', mode="a",  if_sheet_exists="replace")
df.to_excel(writer, sheet_name = 'Metadata', index=False)
writer.close()