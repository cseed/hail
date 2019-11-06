import os
from urllib.parse import urlparse, urlunparse, ParseResult
from typing import Dict, List, Optional

from .fs import FS
from .hadoop_fs import HadoopFS
from .local_fs import LocalFS
from .google_fs import GoogleCloudStorageFS


class BaseFS(FS):
    def __init__(self, default_scheme: Optional[str], use_hadoop_fs: bool,
                 use_native_local_fs: bool, use_native_gs_fs: bool):
        if 'HAIL_FS_DEFAULT_SCHEME' in os.environ:
            default_scheme = os.environ['HAIL_FS_DEFAULT_SCHEME']

        if 'HAIL_FS_USE_HADOOP_FS' in os.environ:
            use_hadoop_fs = os.environ['HAIL_FS_USE_HADOOP_FS'] == '1'

        if 'HAIL_FS_USE_NATIVE_LOCAL_FS' in os.environ:
            use_native_local_fs = os.environ['HAIL_FS_USE_NATIVE_LOCAL_FS'] == '1'

        if 'HAIL_FS_USE_NATIVE_GS_FS' in os.environ:
            use_native_gs_fs = os.environ['HAIL_FS_USE_NATIVE_GS_FS'] == '1'

        self._default_scheme = default_scheme
        self._filesystems = {}

        # hadoop is the default
        if use_hadoop_fs:
            self._hadoop_fs = HadoopFS()
        else:
            self._hadoop_fs = None

        if use_native_local_fs:
            self._filesystems['file'] = LocalFS()

        if use_native_gs_fs:
            self._filesystems['gs'] = GoogleCloudStorageFS()

    def _filesystem_and_normalized_path(self, path: str):
        result = urlparse(path)

        scheme = result.scheme
        if not scheme and self._default_scheme:
            scheme = self._default_scheme

        fs = self._filesystems.get(scheme, self._hadoop_fs)
        if not fs:
            raise ValueError(f'no file system for scheme {scheme}')

        # normalize path
        if isinstance(fs, LocalFS):
            result = ParseResult('', *result[1:])
        else:
            result = ParseResult(scheme, *result[1:])

        return fs, urlunparse(result)

    def open(self, path: str, mode: str = 'r', buffer_size: int = 2**18):
        fs, path = self._filesystem_and_normalized_path(path)
        return fs.open(path, mode, buffer_size)

    def copy(self, src: str, dest: str):
        src_fs, src = self._filesystem_and_normalized_path(src)
        dest_fs, dest = self._filesystem_and_normalized_path(dest)

        if src_fs == dest_fs:
            src_fs.copy(src, dest)
            return

        with src_fs.open(src, 'rb') as srcf:
            with dest_fs.open(dest, 'wb') as destf:
                shutil.copyfileobj(srcf, destf)

    def exists(self, path: str) -> bool:
        fs, path = self._filesystem_and_normalized_path(path)
        return fs.exists(path)

    def is_file(self, path: str) -> bool:
        fs, path = self._filesystem_and_normalized_path(path)
        return fs.is_file(path)

    def is_dir(self, path: str) -> bool:
        fs, path = self._filesystem_and_normalized_path(path)
        return fs.is_dir(path)

    def stat(self, path: str) -> Dict:
        fs, path = self._filesystem_and_normalized_path(path)
        return fs.stat(path)

    def ls(self, path: str) -> List[Dict]:
        fs, path = self._filesystem_and_normalized_path(path)
        return fs.ls(path)
