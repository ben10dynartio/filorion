from filorion.filestorage import FileStorage
from pathlib import Path

class LocalFileStorage(FileStorage):
    def __init__(self, **kwargs):
        self.utilstack = "local"
        self.p = kwargs

        for myp in ["path"]:
            if myp not in self.p:
                print("self.p =", self.p)
                raise AttributeError(f"Missing {myp} in LocalFileStorage client init")


    def delete_all():
        modules_local_copy = os.listdir(self.p["path"])
        for module in modules_local_copy:
            mypath = Path(self.p["path"]) / module
            shutil.rmtree(mypath)