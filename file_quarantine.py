import boto3
import os
import pandas as pd
from bento.common.utils import get_stream_md5, get_logger, LOG_PREFIX, APP_NAME
import sys

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'file_quarantine'
    os.environ[APP_NAME] = 'file_quarantine'

def get_hash_md5(s3, s3_bucket, s3_file_key):
    s3_obj = s3.get_object(Bucket=s3_bucket, Key=s3_file_key)
    s3_stream = s3_obj['Body']
    s3_hash = get_stream_md5(s3_stream)
    return s3_hash

def move_to_quarantine(quarantine_tsv, quarantine_bucket, delete_original_file, log):
    s3 = boto3.client('s3')
    quarantine_data = pd.read_csv(quarantine_tsv, sep='\t')
    for index, row in quarantine_data.iterrows():
        s3_uri = row["file_url_in_cds"]
        if not s3_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI, {s3_uri}")
        s3_path = s3_uri[5:]
        s3_bucket, _, s3_file_key = s3_path.partition('/')
        try:
            #copy the file to the quarantine bucket
            s3.copy_object(
                Bucket = quarantine_bucket,
                CopySource = {'Bucket': s3_bucket, 'Key': s3_file_key},
                Key = s3_file_key
            )
            #varify the copy integrity
            source_md5 = get_hash_md5(s3, s3_bucket, s3_file_key)
            copy_md5 = get_hash_md5(s3, quarantine_bucket, s3_file_key)
            if source_md5 == copy_md5:
                log.info(f"MD5 validation passed, moved {s3_file_key} from the s3 bucket {s3_bucket} to the quarantine bucket {quarantine_bucket} successfully")
                #delete the original file
                if delete_original_file:
                    s3.delete_object(Bucket=s3_bucket, Key=s3_file_key)
                    log.info(f"{s3_file_key} was deleted from the bucket {s3_bucket}")
            else:
                log.error(f"Copied file {s3_file_key} validation fail, abort file_copying")
                sys.exit(1)
        except Exception as e:
            log.error(e)

if __name__ == "__main__":
    log = get_logger('S3 File Quarantine')
    quarantine_tsv = "tests/test_quarantine.tsv"  #quarantine TSV file with "file_url_in_cds" column inside
    quarantine_bucket = "quarantine-bucket"  #quarantine bucket name
    delete_original_file = True #whether or not delete the original_file
    
    move_to_quarantine(quarantine_tsv, quarantine_bucket, delete_original_file, log)