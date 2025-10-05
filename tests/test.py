from miniorion import MinioFileStorage
from __init__ import FileStorage
from testkeys.testkey import testkeys, pathfile1_o, pathfile1_d
"""
testkeys = {"bucket_name":"xxxx",
            "endpoint":"xxxx:9000",
            "access_key":"xxxx",
            "secret_key":"xxxxzzzzzzzzz",
            "secure":True}
"""

fileclient = MinioFileStorage(**testkeys)

def push_file_to_minio():
    fileclient.push_file(pathfile1_o, pathfile1_d)

push_file_to_minio()