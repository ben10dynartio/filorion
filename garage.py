from filorion.minio import MinioFileStorage
from minio import Minio
import urllib3

class GarageFileStorage(MinioFileStorage):
    def __init__(self, **kwargs):
        self.utilstack = "garage"
        self.p = kwargs

        for myp in ["endpoint", "access_key", "secret_key", "bucket_name"]:
            if myp not in self.p:
                print("self.p =", self.p)
                raise AttributeError(f"Missing {myp} in FileStorage client init with Garage")
        self.p["secure"] = self.p.get("secure", True)
        self.p["cert_reqs"] = self.p.get("cert_reqs", True)

        if self.p["cert_reqs"]:
            self.client = Minio(
                self.p["endpoint"],
                access_key=self.p["access_key"],
                secret_key=self.p["secret_key"],
                secure=self.p["secure"],
                region="garage"
            )
        else:
            self.client = Minio(
                self.p["endpoint"],
                access_key=self.p["access_key"],
                secret_key=self.p["secret_key"],
                secure=self.p["secure"],
                http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
                region="garage"
                # (num_pools=10,
                # ca_certs=r'/home/ben/Sinamet/lib/sinametx/minio/myfile.crt',
                # cert_reqs="CERT_REQUIRED",
                # cert_reqs="CERT_NONE",),
            )