'''
This script is used to monitor files generated from Sentinel1 pre-process script.
Upload newly created files to S3.
Once there is no more new files, send message to cellphone using library twilio.
'''

import boto3
import os
import shutil
import time
from twilio.rest import Client


# File status check (Linux)
class File(object):

    def __init__(self, file_path):

        if not os.path.exists(file_path):
            raise OSError('{file_path} not exist'.format(file_path=file_path))

        self.file_path = os.path.abspath(file_path)

    def status(self):

        open_fd_list = self.__get_all_fd()

        open_count = len(open_fd_list)

        is_opened = False

        if open_count > 0:
            is_opened = True

        return {'is_opened': is_opened, 'open_count': open_count}

    def __get_all_pid(self):

        """获取当前所有进程 Get all the current process"""

        return [_i for _i in os.listdir('/proc') if _i.isdigit()]

    def __get_all_fd(self):

        """获取所有已经打开该文件的fd路径 Get all pathes that is using this file"""

        all_fd = []

        for pid in self.__get_all_pid():

            _fd_dir = '/proc/{pid}/fd'.format(pid=pid)

            if os.access(_fd_dir, os.R_OK) == False:
                continue

            for fd in os.listdir(_fd_dir):

                fd_path = os.path.join(_fd_dir, fd)

                if os.path.exists(fd_path) and os.readlink(fd_path) == self.file_path:
                    all_fd.append(fd_path)

        return all_fd

# upload function
def upload_folder_to_s3(s3bucket, inputDir, s3Path):
    print("Uploading results to s3 initiated...")
    print("Local Source:",inputDir)
    os.system("ls -ltR " + inputDir)

    print("Dest S3path:",s3Path)

    try:
        for path, subdirs, files in os.walk(inputDir):
            for file in files:
                if file.endswith('.csv'):
                    dest_path = path.replace(inputDir,"")
                    __s3file = os.path.normpath(s3Path + '/' + dest_path + '/' + file)
                    __local_file = os.path.join(path, file)
                    print("upload : ", __local_file, " to Target: ", __s3file, end="")
                    s3bucket.upload_file(__local_file, __s3file)
                    print(" ...Success")
                else:
                    dest_path = path.replace(inputDir,"")
                    __s3file = os.path.normpath(s3Path + '/' + dest_path + '/' + file)
                    __local_file = os.path.join(path, file)
                    print("upload : ", __local_file, " to Target: ", __s3file, end="")
                    s3bucket.upload_file(__local_file, __s3file)
                    print(" ...Success")
    except Exception as e:
        print(" ... Failed!! Quitting Upload!!")
        print(e)
        raise e

# Check before remove
def check_local_data(objMainName):
    temp_s1l2_data = objMainName + '.data'
    temp_s1l2_dim = objMainName + '.dim'
    
    temp_s1l1_safe = objMainName + '.SAFE'
    temp_s1l1_zip = objMainName + '.zip'
    
    flag = 0
    
    # S1L2_fpath
    _data_pth = os.path.join(S1L2_fpath,temp_s1l2_data)
    if os.path.exists(_data_pth) and os.path.isdir(_data_pth):
        flag = 1
    else: 
        flag =0
    
    _dim_pth = os.path.join(S1L2_fpath,temp_s1l2_dim)
    if os.path.exists(_dim_pth):
        flag = 1
    else: 
        flag =0
    
    # S1L1_fpath
    _SAFE_pth = os.path.join(S1L1_fpath,temp_s1l1_safe)
    if os.path.exists(_SAFE_pth) and os.path.isdir(_SAFE_pth):
        flag = 1
    else: 
        flag =0
    
    _zip_pth = os.path.join(S1L1_fpath,temp_s1l1_zip)
    if os.path.exists(_zip_pth):
        flag = 1
    else: 
        flag =0
    
    return flag

# Remove .dim .data in Sentinel1_level2 and .zip .SAFE in Sentinel1_level2
def remove_local_data(objMainName):
    
    temp_s1l2_data = objMainName + '.data'
    temp_s1l2_dim = objMainName + '.dim'
    
    temp_s1l1_safe = objMainName + '.SAFE'
    temp_s1l1_zip = objMainName + '.zip'
    
    # S1L2_fpath
    # remove .data
    _data_pth = os.path.join(S1L2_fpath,temp_s1l2_data)
    if os.path.exists(_data_pth) and os.path.isdir(_data_pth):
        print('Removing folder: ',_data_pth)
        shutil.rmtree(_data_pth)
    # remove .dim
    _dim_pth = os.path.join(S1L2_fpath,temp_s1l2_dim)
    if os.path.exists(_dim_pth):
        print('Removing file: ',_dim_pth)
        os.remove(_dim_pth)
    
    # S1L1_fpath
    # remove .SAFE
    _SAFE_pth = os.path.join(S1L1_fpath,temp_s1l1_safe)
    if os.path.exists(_SAFE_pth) and os.path.isdir(_SAFE_pth):
        print('Removing folder: ',_SAFE_pth)
        shutil.rmtree(_SAFE_pth)
    # remove .zip
    _zip_pth = os.path.join(S1L1_fpath,temp_s1l1_zip)
    if os.path.exists(_zip_pth):
        print('Removing file: ',_zip_pth)
        os.remove(_zip_pth)

# Get the status of desired files.
def get_all_status(filepath):
    flag_bucket = []
    '''loop through all file to get file status and push them to flag bucket'''
    for file in os.listdir(filepath):
        temp_fpath = os.path.join(filepath, file)
        temp_file = File(temp_fpath)
        flag_bucket.append(temp_file.status()["is_opened"])

    '''check if any file is wirtten'''
    flag = False
    if True in flag_bucket:
        flag = False
    #         print('DoneProcess is still running')
    else:
        flag = True
    #         print("Done")
    '''return status'''
    return flag


def runMonitor():
    Raw_data_list = os.listdir(S1L1_fpath)
    Raw_data_list.remove('.ipynb_checkpoints')

    while len(Raw_data_list) != 0:
        # Read objs
        local_files = os.listdir(S1L2_fpath)
        local_files.remove('.ipynb_checkpoints')

        # Keep .data and Exclude .dim files
        target = []
        for name in local_files:
            if name.endswith('.data'):
                target.append(name)

        for obj in target:
            print('Excuting: ', obj)
            temp_S1L2_fpath = os.path.join(S1L2_fpath, obj)
            # print('file path : ',temp_S1L2_fpath)
            file_len = len(os.listdir(temp_S1L2_fpath))
            if file_len == 5:
                temp_fname = obj[:-5]
                # print('temp_fname: ',temp_fname)
                if check_local_data(temp_fname):
                    print('----The path of tile folder is correct.......文件完整')
                    '''start checking file status'''
                    print('----Start Checking File Status.......')
                    _filestatus = get_all_status(temp_S1L2_fpath)
                    if _filestatus:
                        print('----Files are created correctly!')
                        # excute upload
                        temp_s3fpth = os.path.join(s3_fpath, obj)[:-5]
                        print('******Start Uploading******')
                        upload_folder_to_s3(s3bucket,temp_S1L2_fpath,temp_s3fpth)
                        # remove files. 4 items for each target.
                        temp_fname = obj[:-5]
                        remove_local_data(temp_fname)
                        print('******Finish !******', '\n')
                    else:
                        # do nothing
                        print('----Files are still occupied by the process!')
                        print('----Waiting for next upload window.....', '\n')

                else:
                    # do nothing
                    print('----do nothing', '\n')
            else:
                # do nothing
                print('----do nothing', '\n')

        time.sleep(1800)
    else:
        # Send message
        account_sid = ''
        auth_token = ''
        client = Client(account_sid, auth_token)

        message = client.messages \
            .create(
            body="Preprocessing for Sentinel 1 is finished.",
            from_='',
            to=''
        )

        print(message.sid)


ACCESS_KEY_ID = ''
SECRET_ACCESS_KEY = ''
REGION = 'us-east-1'
BUCKET = ''

s3 = boto3.resource('s3',
                        aws_access_key_id = ACCESS_KEY_ID,
                        aws_secret_access_key = SECRET_ACCESS_KEY
                        )
s3bucket = s3.Bucket(BUCKET)

s3_fpath = "Sentinel_Zhenhua/S1_level2"
local_fpath = "/home/ubuntu/SentinelProject"
S1L1_fpath = os.path.join(local_fpath,'sentinel1_level1')
S1L2_fpath = os.path.join(local_fpath,'sentinel1_level2')

runMonitor()
