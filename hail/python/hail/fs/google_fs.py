import os
from stat import S_ISREG, S_ISDIR
from typing import Dict, List
import gcsfs
from hurry.filesize import size
from shutil import copy2

from .fs import FS


class GoogleCloudStorageFS(FS):
    def __init__(self):
        self.client = gcsfs.core.GCSFileSystem(
            token='google_default',
            secure_serialize=True)

    def _add_gs_path_prefix(self, path: str) -> str:
        first_idx = 0

        for char in path:
            if(char != "/"):
                break
            first_idx += 1

        return "gs://" + path[first_idx:]

    def open(self, path: str, mode: str = 'r', buffer_size: int = 2**18):
        return self.client.open(path, mode, buffer_size)

    def copy(self, src: str, dest: str):
        self.client.copy(src, dest)

    def exists(self, path: str) -> bool:
        return self.client.exists(path)

    def is_file(self, path: str) -> bool:
        try:
            return not self._stat_is_gs_dir(self.client.info(path))
        except FileNotFoundError:
            return False

    def is_dir(self, path: str) -> bool:
        try:
            return self._stat_is_gs_dir(self.client.info(path))
        except FileNotFoundError:
            return False

    def stat(self, path: str) -> Dict:
        return self._format_stat_gs_file(self.client.info(path))

    def _format_stat_gs_file(self, stats: Dict) -> Dict:
        return {
            'is_dir': self._stat_is_gs_dir(stats),
            'size_bytes': stats['size'],
            'size': size(stats['size']),
            'path': self._add_gs_path_prefix(stats['path']),
            'owner': stats['bucket'],
            'modification_time': stats.get('updated')
        }

    def _stat_is_gs_dir(self, stats: Dict) -> bool:
        return stats['storageClass'] == 'DIRECTORY' or stats['name'].endswith('/')

    def ls(self, path: str) -> List[Dict]:
        return [self._format_stat_gs_file(file) for file in self.client.ls(path, detail=True)]
