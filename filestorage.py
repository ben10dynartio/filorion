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

    def reset_storage(self, verbose=False):
        """Supprime tous les objets existants dans le bucket"""
        pass

    def remove_file(self, filepath):
        """Supprime un objet du bucket."""
        pass
