from filestorage import FileStorage
from minio import Minio
from func import calculate_file_hash
from urllib3.exceptions import MaxRetryError
import urllib3
from pathlib import Path

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


    def push_file(self, source_file, destination_file):
        """Send a file to MinIO."""
        local_hash = calculate_file_hash(source_file)
        metadata = {'hashcode': local_hash}
        self.client.fput_object(self.p["bucket_name"], destination_file, source_file, metadata=metadata)