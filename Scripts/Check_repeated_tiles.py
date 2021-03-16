'''
This script is used to check if there are repeated image tiles on local.
If so, find and remove them from local
'''
import boto3
import os
import pandas

ACCESS_KEY_ID = ' '
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

# Path of query folder
file_prefix = "Sentinel_Zhenhua/S1_level2/"


result = []
for object_summary in bucket.objects.filter(Prefix = file_prefix):
    # print(object_summary)
    object = object_summary.key  # get file names from the folder
    # print(object)
    if object.endswith('.csv'):
        folder_name = object.split("/")[2]
        # print(object)
        result.append([str(folder_name)])

df = pandas.DataFrame.from_dict(result)
# print('Before dropping duplicated rows, nrows = ', df.count())
df = df.drop_duplicates()
# print('After dropping duplicated rows, nrows = ', df.count())

# Path of local folder
local_fpath = "/home/ubuntu/SentinelProject"
S1L1_fpath = os.path.join(local_fpath,'sentinel1_level1')

for tilename in df[0]:
    tmp_tileOBJ = tilename+'.zip'
    if(os.path.exists(os.path.join(S1L1_fpath,tmp_tileOBJ))):
        print('Removing :', tmp_tileOBJ)
        os.remove(os.path.join(S1L1_fpath,tmp_tileOBJ))
    else:
        print('Safe')
        pass