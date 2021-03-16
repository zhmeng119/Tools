'''
This script is used  to copy Sentinel1-level2 images from S3 to local
'''

import boto3
import os
import pandas

ACCESS_KEY_ID = ''
SECRET_ACCESS_KEY = ''
REGION = 'us-east-1'
BUCKET = 'activemapper'

# S3 config
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


vv_name = "Sigma0_VV_db.img"
vv_hdr = "Sigma0_VV_db.hdr"
vh_name = "Sigma0_VH_db.img"
vh_hdr = "Sigma0_VH_db.hdr"
local_pth = 'A/B/C/D/Local_path'
for tile in df[0]:
    s3_vv_pth = 'activemapper/Sentinel_Zhenhua/S1_level2/'+tile+'/'+vv_name
    s3_vv_hdr_pth = 'activemapper/Sentinel_Zhenhua/S1_level2/'+tile+'/'+vv_hdr
    local_vv_pth = local_pth+'/S1_level2/'+tile+'.data'+'/'+vv_name
    local_vv_hdr_pth = local_pth+'/S1_level2/'+tile+'.data'+'/'+vv_hdr

    s3_vh_pth = 'activemapper/Sentinel_Zhenhua/S1_level2/'+tile+'/'+vh_name
    s3_vh_hdr_pth = 'activemapper/Sentinel_Zhenhua/S1_level2/'+tile+'/'+vh_hdr
    local_vh_pth = local_pth+'/S1_level2/'+tile+'.data'+'/'+vh_name
    local_vh_hdr_pth = local_pth+'/S1_level2/'+tile+'.data'+'/'+vh_hdr
    print('-----------------------')
    print('aws s3 scp %s %s' % ('s3://'+s3_vv_pth,local_vv_pth))
    print('aws s3 scp %s %s' % ('s3://' + s3_vv_hdr_pth, local_vv_hdr_pth))
    print('**')
    print('aws s3 scp %s %s' % ('s3://'+s3_vh_pth,local_vh_pth))
    print('aws s3 scp %s %s' % ('s3://' + s3_vh_hdr_pth, local_vh_hdr_pth))
    os.system('aws s3 scp %s %s' % ('s3://'+s3_vv_pth,local_vv_pth))
    os.system('aws s3 scp %s %s' % ('s3://' + s3_vv_hdr_pth, local_vv_hdr_pth))
    print('-----------------------')



