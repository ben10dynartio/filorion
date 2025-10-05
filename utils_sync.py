from pathlib import Path
from urllib3.exceptions import MaxRetryError

def sync(self, bucketname, destination_path, verbose=False):
    """Synchronise les fichiers d'un bucket avec un dossier local."""

    if verbose: print("Sync client = %s" % self.client)

    if self.utilstack == "minio":
        try:
            lst_objects = self.client.list_objects(bucketname, recursive=True)
        except MaxRetryError:
            return False
        miniopath = Path(destination_path)
        for obj in lst_objects:
            # print("Move file %s to %s | " % (obj, miniopath / obj.object_name), end="")
            doc = self.client.fget_object(bucketname, obj.object_name, miniopath / obj.object_name)
            # print(doc)
        return True
    else:
        raise ValueError("Unknwon stack")