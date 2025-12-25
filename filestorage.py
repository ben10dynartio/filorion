from datetime import timedelta
from minio import Minio
from pathlib import Path
from urllib3.exceptions import MaxRetryError
import urllib3
import shutil
import os
from filorion.func import calculate_file_hash

class FileStorage:

    def __init__(self, **kwargs):
        self.utilstack = "undefined"

    def push_folder(self, source_folder, destination_folder):
        pass

    def push_file(self, source_file, destination_file):
        """Send a file to FileStorage"""
        pass

    def get_list_files(self, prefix=None, recursive=False):
        """Liste les objets dans le bucket spécifié."""
        pass

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