'''
Batch down for Image folder
'''
import yaml
import boto3
import os

ACCESS_KEY_ID = ''
SECRET_ACCESS_KEY = ''
REGION = 'us-east-1'
BUCKET = ''

# boto3 client
client = boto3.client(
    's3',
    aws_access_key_id = ACCESS_KEY_ID,
    aws_secret_access_key = SECRET_ACCESS_KEY
)

bucket = boto3.resource('s3',
                        aws_access_key_id = ACCESS_KEY_ID,
                        aws_secret_access_key = SECRET_ACCESS_KEY
                        ).Bucket(BUCKET)


file_prefix = "classified-images/2_Retrain_700_10samples/"
# file_prefix = "classified-images/15_Retrain_700_10samples/"
local_dir = ''


for object_summary in bucket.objects.filter(Prefix = file_prefix):
    object = object_summary.key  # get file names from the folder
    # check print status before download
    print(object)
    if object.endswith('.tif'):
        file_name = object.split("/")[-1]
        local_name = os.path.join(local_dir, file_name)
        client.download_file(bucket.name, object, local_name)




