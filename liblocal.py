from pathlib import Path
import os
import shutil

from filorion.filestorage import FileStorage

class LocalFileStorage(FileStorage):
    def __init__(self, **kwargs):
        self.utilstack = "local"
        self.p = kwargs

        for myp in ["path"]:
            if myp not in self.p:
                print("self.p =", self.p)
                raise AttributeError(f"Missing {myp} in LocalFileStorage client init")

    def delete_all(self):
        modules_local_copy = os.listdir(self.p["path"])
        for module in modules_local_copy:
            mypath = Path(self.p["path"]) / module
            shutil.rmtree(mypath)

    remove_all = delete_all

    def remove_file(self, filepath):
        """Supprime un objet du bucket."""
        self.client.remove_object(self.p["bucket_name"], filepath)
        return True
