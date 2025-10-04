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

class FileStorage():

    def __init__(self, utilstack, *args, **kwargs):
        if utilstack.lower() not in ['minio']:
            raise AttributeError("Unknown utilstack '%s'" % utilstack)
        if len(args)==1:
            if type(args[0]) is dict:
                endpoint, access_key, secret_key, secure = args[0]["endpoint"], args[0]["access_key"], args[0]["secret_key"], args[0]["secure"]
                cert_reqs = args[0].get("cert_reqs", True)
        elif len(args)==4:
            endpoint, access_key, secret_key, secure = args
            cert_reqs = True
        elif len(args)==5:
            endpoint, access_key, secret_key, secure, cert_reqs = args
        else:
            endpoint, access_key, secret_key, secure = kwargs.get("endpoint"), kwargs.get("access_key"), kwargs.get("secret_key"), kwargs.get("secure")
            cert_reqs = kwargs.get("cert_reqs", True)
            #raise AttributeError("Incorrect config infos, args = %s | kwargs = %s" % (args, kwargs))
        if cert_reqs:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                #http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
                #(num_pools=10,
                #ca_certs=r'/home/ben/Sinamet/lib/sinametx/minio/CA_SIGMAZ.crt',
                #cert_reqs="CERT_REQUIRED",
                #cert_reqs="CERT_NONE",),
            )
        else:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
                # (num_pools=10,
                # ca_certs=r'/home/ben/Sinamet/lib/sinametx/minio/CA_SIGMAZ.crt',
                # cert_reqs="CERT_REQUIRED",
                # cert_reqs="CERT_NONE",),
            )
        self.utilstack = utilstack.lower()

    def push_file(self, bucket_name, destination_file, source_file):
        """Envoie un fichier vers MinIO."""
        if self.utilstack == 'minio':
            local_hash = self.calculate_file_hash(source_file)
            metadata = {'hashcode': local_hash} 
            self.client.fput_object(bucket_name, destination_file, source_file, metadata=metadata)
             
    def calculate_file_hash(self, file_path):
        """Calcule le hash MD5 d'un fichier local."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    

    def get_hashcode(self, bucket_name, file_path):
        """Récupère le hashcode d'un fichier dans MinIO à partir de ses métadonnées."""
        try:
            obj = self.client.stat_object(bucket_name, file_path)
            return  obj.metadata.get('hashcode', None)  
        except Exception as e:
            return None  
             
    def check_utilstack(self):
        """Vérifie si le utilstack est bien 'minio' avant d'exécuter une opération."""
        if self.utilstack != "minio":
            raise ValueError(f" L'opération n'est pas supportée pour utilstack '{self.utilstack}'.")

    def ensure_bucket(self, bucket_name):
        """Vérifie si le bucket existe, sinon le crée."""
        if self.utilstack == 'minio':
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
        else:
            raise ValueError("Unknwon stack")

    def reset_bucket(self, bucket_name, verbose=False):
        """Supprime tous les objets existants dans le bucket"""
        if verbose: print(f"🔄 Réinitialisation du bucket '{bucket_name}' en cours...")
        objects_to_delete = list(self.get_list_files(bucket_name, recursive=True))
        if verbose:
            if not objects_to_delete:
                print(f"✅ Aucun fichier trouvé dans le bucket '{bucket_name}'. Rien à supprimer.")
        else:
            print(f"🔍 {len(objects_to_delete)} fichiers trouvés à supprimer...")
        for obj in objects_to_delete:
            self.remove_file(bucket_name, obj.object_name)
        if verbose: print("✅ Tous les fichiers ont été supprimés.")

    def get_list_files(self, bucket_name, prefix=None, recursive=False):
        """Liste les objets dans le bucket spécifié."""
        if self.utilstack == 'minio':
            return list(self.client.list_objects(bucket_name, prefix=prefix, recursive=recursive))
        else:
            raise ValueError("Unknwon stack")
    
    def remove_file(self, bucket_name, object_name):
        """Supprime un objet du bucket."""
        if self.utilstack == 'minio':
            self.client.remove_object(bucket_name, object_name)
            return True
        else:
            raise ValueError("Unknwon stack")

    def get_share_link(self, bucket_name, file_path, expire_time_min=60):
        """Génère un lien de partage pour un fichier."""
        if self.utilstack == 'minio':
            url = self.client.presigned_get_object(
                bucket_name, file_path, expires=timedelta(minutes=expire_time_min))
            """url = self.client.get_presigned_url(
                "GET", bucket_name, file_path, expires=timedelta(minutes=expire_time_min))"""
        return url

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
                #print("Move file %s to %s | " % (obj, miniopath / obj.object_name), end="")
                doc = self.client.fget_object(bucketname, obj.object_name, miniopath / obj.object_name)
                #print(doc)
            return True
        else:
            raise ValueError("Unknwon stack")

    def get_list_objects_names(self):
        """Liste les objets dans le bucket spécifié."""

        if self.utilstack == "minio":
            try:
                # return [str(c) for c in client.list_buckets()]
                lst_objects = self.client.list_objects('sinamet-modules', recursive=True)
                return [obj.object_name for obj in lst_objects]
            except MaxRetryError as e:
                print("MinIO Error for 'minioclient=%s/bucketname=%s'" % (self.client, 'sinamet-modules'))
                print(e)
                return ["MinIO Connection Error !"]
        else:
            raise ValueError("Unknwon stack")

    def get_list_modules_names(self, bucketname):
        if self.utilstack == "minio":
            try:
                # return [str(c) for c in client.list_buckets()]
                lst_objects = self.client.list_objects(bucketname)
                return [obj.object_name[:-1] for obj in lst_objects]
            except MaxRetryError as e:
                print("MinIO Error for 'minioclient=%s/bucketname=%s'" % (self.client, bucketname))
                print(e)
                return ["MinIO Connection Error !"]
        else:
            raise ValueError("Unknwon stack")

    @staticmethod
    def reset_local_copy(destination_path):
        modules_local_copy = os.listdir(destination_path)
        for module in modules_local_copy:
            mypath = Path(destination_path) / module
            shutil.rmtree(mypath)

if __name__ == "__main__":

    pass