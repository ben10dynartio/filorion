from filorion import GarageFileStorage

from testkeys.testkey import testkeys, pathfile1_o, pathfile1_d, pathfolder1_o, pathfolder1_d
"""
testkeys = {"bucket_name":"xxxx",
            "endpoint":"xxxx:9000",
            "access_key":"xxxx",
            "secret_key":"xxxxzzzzzzzzz",
            "secure":True}
"""

fileclient = GarageFileStorage(cert_reqs=False, **testkeys)
print(fileclient)

fileclient.get_all_files("testlocal/")
#fileclient.push_file(pathfile1_o, pathfile1_d)
#fileclient.push_folder(pathfolder1_o, pathfolder1_d)