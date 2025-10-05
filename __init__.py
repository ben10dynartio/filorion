from datetime import timedelta
from minio import Minio
from pathlib import Path
from urllib3.exceptions import MaxRetryError
import urllib3
import shutil
import os
import hashlib

 # Create client with access key and secret key
 # :todo: Embedding configuration

def calculate_file_hash(file_path):
    """Comput MD5 hash of a local file"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class FileStorage:

    def __init__(self, utilstack, **kwargs):
        self.utilstack = utilstack.lower()
        if utilstack.lower() not in ['minio']:
            raise AttributeError("Unknown utilstack '%s'" % utilstack)

        self.p = kwargs

        if utilstack.lower() == 'minio':
            for myp in ["endpoint", "access_key", "secret_key", "bucket_name"]:
                if myp not in self.p:
                    raise AttributeError("Missing {myp} in FileStorage client init with MinIO")
            self.p["secure"] = self.p.get("secure", True)
            self.p["cert_reqs"] = self.p.get("cert_reqs", True)


        if self.p["cert_reqs"]:
            self.client = Minio(
                self.p["enpoint"],
                access_key=self.p["access_key"],
                secret_key=self.p["secret_key"],
                secure=self.p["secure"],
            )
        else:
            self.client = Minio(
                self.p["enpoint"],
                access_key=self.p["access_key"],
                secret_key=self.p["secret_key"],
                secure=self.p["secure"],
                http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
                # (num_pools=10,
                # ca_certs=r'/home/ben/Sinamet/lib/sinametx/minio/myfile.crt',
                # cert_reqs="CERT_REQUIRED",
                # cert_reqs="CERT_NONE",),
            )

    def push_file(self, source_file, destination_file):
        """Send a file to MinIO."""
        if self.utilstack == 'minio':
            local_hash = calculate_file_hash(source_file)
            metadata = {'hashcode': local_hash} 
            self.client.fput_object(self.p["bucket_name"], destination_file, source_file, metadata=metadata)

    def get_list_files(self, prefix=None, recursive=False):
        """Liste les objets dans le bucket spécifié."""
        if self.utilstack == 'minio':
            try:
                return list(self.client.list_objects(self.p["bucket_name"], prefix=prefix, recursive=recursive))
            except MaxRetryError as e:
                print("MinIO Error for 'minioclient=%s/bucketname=%s'" % (self.client, 'sinamet-modules'))
                raise e
        else:
            raise ValueError("Unknwon stack")

    def get_hashcode(self, bucket_name, file_path):
        """Récupère le hashcode d'un fichier dans MinIO à partir de ses métadonnées."""
        try:
            obj = self.client.stat_object(self.p["bucket_name"], file_path)
            return  obj.metadata.get('hashcode', None)  
        except Exception as e:
            return None

    """def ensure_bucket(self, bucket_name):
        ""Vérifie si le bucket existe, sinon le crée.""
        if self.utilstack == 'minio':
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
        else:
            raise ValueError("Unknwon stack")"""

    def reset_storage(self, verbose=False):
        """Supprime tous les objets existants dans le bucket"""
        if verbose: print(f"🔄 Réinitialisation du bucket '{self.p["bucket_name"]}' en cours...")
        objects_to_delete = list(self.get_list_files(self.p["bucket_name"], recursive=True))
        if verbose:
            if not objects_to_delete:
                print(f"✅ Aucun fichier trouvé dans le bucket '{self.p["bucket_name"]}'. Rien à supprimer.")
            else:
                print(f"🔍 {len(objects_to_delete)} fichiers trouvés à supprimer...")
        for obj in objects_to_delete:
            self.remove_file(obj.object_name)
        if verbose: print("✅ Tous les fichiers ont été supprimés.")

    def remove_file(self, filepath):
        """Supprime un objet du bucket."""
        if self.utilstack == 'minio':
            self.client.remove_object(self.p["bucket_name"], filepath)
            return True
        else:
            raise ValueError("Unknwon stack")

    def get_share_link(self, file_path, expire_time_min=60):
        """Génère un lien de partage pour un fichier."""
        url = ""
        if self.utilstack == 'minio':
            url = self.client.presigned_get_object(
                self.p["bucket_name"], file_path, expires=timedelta(minutes=expire_time_min))
            """url = self.client.get_presigned_url(
                "GET", bucket_name, file_path, expires=timedelta(minutes=expire_time_min))"""
        return url


if __name__ == "__main__":
    pass