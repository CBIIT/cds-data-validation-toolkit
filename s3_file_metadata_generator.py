import boto3
import csv
from bento.common.utils import get_stream_md5
import os

def get_all_files_and_metadata(bucket_name, directory_prefix="", aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None):
    """
    Lists all files under a specified S3 directory prefix and retrieves metadata for each.
    """
    s3_client = boto3.client('s3', 
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,
                             aws_session_token=aws_session_token)
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=directory_prefix)
    
    file_metadata_list = []
    file_sep = "/"

    for page in pages:
        if 'Contents' in page:
            for file_object in page['Contents']:
                object_key = file_object['Key']
                if object_key == directory_prefix and directory_prefix.endswith('/'):
                    continue

                print(f"Retrieving metadata for: {object_key}")
                try:
                    metadata_response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
                    s3_file = s3_client.get_object(Bucket=bucket_name, Key=object_key)
                    s3_file_content = s3_file['Body']
                    s3_hash = get_stream_md5(s3_file_content)
                    # Extract common useful fields
                    extracted_data = {
                        'file_url_in_cds': f"s3://{bucket_name}{file_sep}{object_key}",
                        'md5sum': s3_hash,
                        'Key': object_key,
                        'file_name': object_key.replace(directory_prefix+"/",""),
                        'file_size': metadata_response.get('ContentLength'),
                        'file_type': os.path.splitext(object_key)[1].lstrip('.').upper(),
                        'LastModified': metadata_response.get('LastModified'),
                        'ETag': metadata_response.get('ETag').strip('"'), # Remove quotes from ETag
                        'ContentType': metadata_response.get('ContentType'),
                        # Add custom metadata fields if you use them (e.g., metadata_response.get('Metadata', {}).get('my-custom-header'))
                    }
                    file_metadata_list.append(extracted_data)

                except Exception as e:
                    print(f"Failed to retrieve metadata for {object_key}: {e}")
                    
    return file_metadata_list

def export_to_tsv(data_list, filename="s3_metadata.tsv"):
    """
    Exports a list of dictionaries to a TSV file.
    """
    if not data_list:
        print("No data to export.")
        return

    # Use the keys from the first dictionary as the header
    headers = data_list[0].keys()

    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as tsvfile:
            writer = csv.DictWriter(tsvfile, fieldnames=headers, delimiter='\t')
            
            writer.writeheader()
            writer.writerows(data_list)
        print(f"\nSuccessfully exported {len(data_list)} records to {filename}")
    except IOError as e:
        print(f"Error writing to file {filename}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during file writing: {e}")


# --- Example Usage ---
bucket_name = "your-bucket-name"  # Replace with your S3 bucket name
directory_prefix = "" # e.g., 'images/' or '' for the whole bucket
aws_access_key_id = ""  # Replace with your AWS access key ID if needed
aws_secret_access_key = ""  # Replace with your AWS secret access key if needed
aws_session_token = None # Replace with your AWS session token if needed, default is None

all_metadata = get_all_files_and_metadata(bucket_name, directory_prefix, aws_access_key_id, aws_secret_access_key, aws_session_token)

if all_metadata:
    export_to_tsv(all_metadata, filename="s3_object_metadata_protocol.tsv")