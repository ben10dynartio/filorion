from pathlib import Path
from urllib3.exceptions import MaxRetryError
import urllib3

from minio import Minio

from filorion.filestorage import FileStorage
from filorion.func import calculate_file_hash



class MinioFileStorage(FileStorage):
    def __init__(self, **kwargs):
        self.utilstack = "minio"
        self.p = kwargs

        for myp in ["endpoint", "access_key", "secret_key", "bucket_name"]:
            if myp not in self.p:
                print("self.p =", self.p)
                raise AttributeError(f"Missing {myp} in FileStorage client init with MinIO")
        self.p["secure"] = self.p.get("secure", True)
        self.p["cert_reqs"] = self.p.get("cert_reqs", True)

        if self.p["cert_reqs"]:
            self.client = Minio(
                self.p["endpoint"],
                access_key=self.p["access_key"],
                secret_key=self.p["secret_key"],
                secure=self.p["secure"],
            )
        else:
            self.client = Minio(
                self.p["endpoint"],
                access_key=self.p["access_key"],
                secret_key=self.p["secret_key"],
                secure=self.p["secure"],
                http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
                # (num_pools=10,
                # ca_certs=r'/home/ben/Sinamet/lib/sinametx/minio/myfile.crt',
                # cert_reqs="CERT_REQUIRED",
                # cert_reqs="CERT_NONE",),
            )

    def push_folder(self, source_folder, destination_folder):
        """Send the folder content to MinIO
        :todo: Improve for recursive folders
        """
        myfolder = Path(source_folder)
        # Push all files
        for fichier in myfolder.iterdir():
            if fichier.is_file():
                self.push_file(str(fichier), str(Path(destination_folder) / fichier.name))


    def push_file(self, source_file, destination_file, error="raise"):
        """Send a file to MinIO."""
        try:
            local_hash = calculate_file_hash(source_file)
            metadata = {'hashcode': local_hash}
            self.client.fput_object(self.p["bucket_name"], destination_file, source_file, metadata=metadata)
        except Exception as e:
            if error=="raise":
                raise e
            if error=="warn":
                print("ERROR with push_file", source_file, "to", destination_file)
                print(e)
            if error=="pass":
                pass


    def get_list_files(self, prefix=None, recursive=False):
        try:
            return list(self.client.list_objects(self.p["bucket_name"], prefix=prefix, recursive=recursive))
        except MaxRetryError as e:
            print("MinIO Error for 'minioclient=%s/bucketname=%s'" % (self.client, self.p["bucket_name"]))
            raise e


    def get_list_folders(self):
        try:
            lst_objects = self.client.list_objects(self.p["bucket_name"])
            return [obj.object_name[:-1] for obj in lst_objects]
        except MaxRetryError as e:
            print("MinIO Error for 'minioclient=%s/bucketname=%s'" % (self.client, self.p["bucket_name"]))
            print(e)
            return ["MinIO Connection Error !"]


    def get_all_files(self, destination_path, verbose=False):
        """Synchronise les fichiers d'un bucket avec un dossier local."""
        if verbose: print("Sync client = %s" % self.client)

        try:
            lst_objects = self.client.list_objects(self.p["bucket_name"], recursive=True)
        except MaxRetryError:
            return False
        miniopath = Path(destination_path)
        print("LST OBJ =", lst_objects)
        for obj in lst_objects:
            print(obj)
            directory = miniopath / obj.object_name
            directory.mkdir(parents=True, exist_ok=True)
            doc = self.client.fget_object(self.p["bucket_name"], obj.object_name, miniopath / obj.object_name)
        return True



