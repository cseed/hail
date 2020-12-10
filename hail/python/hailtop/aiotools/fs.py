from typing import TypeVar, Any, Optional, List, Type, BinaryIO, cast, Set, AsyncIterator
from types import TracebackType
import abc
import os
import os.path
import stat
import shutil
from concurrent.futures import ThreadPoolExecutor
import urllib.parse
from hailtop.utils import blocking_to_async, url_basename, url_join
from .stream import ReadableStream, WritableStream, blocking_readable_stream_to_async, blocking_writable_stream_to_async

AsyncFSType = TypeVar('AsyncFSType', bound='AsyncFS')


class FileStatus(abc.ABC):
    @abc.abstractmethod
    async def size(self) -> int:
        pass

    @abc.abstractmethod
    async def __getitem__(self, key: str) -> Any:
        pass


class FileListEntry(abc.ABC):
    @abc.abstractmethod
    def name(self) -> str:
        pass

    @abc.abstractmethod
    async def url(self) -> str:
        pass

    @abc.abstractmethod
    async def is_file(self) -> bool:
        pass

    @abc.abstractmethod
    async def is_dir(self) -> bool:
        pass

    @abc.abstractmethod
    async def status(self) -> FileStatus:
        pass


class AsyncFS(abc.ABC):
    @abc.abstractmethod
    def schemes(self) -> Set[str]:
        pass

    @abc.abstractmethod
    async def open(self, url: str) -> ReadableStream:
        pass

    @abc.abstractmethod
    async def create(self, url: str) -> WritableStream:
        pass

    @abc.abstractmethod
    async def mkdir(self, url: str) -> None:
        pass

    @abc.abstractmethod
    async def statfile(self, url: str) -> FileStatus:
        pass

    @abc.abstractmethod
    def listfiles(self, url: str, recursive: bool = False) -> AsyncIterator[FileListEntry]:
        pass

    @abc.abstractmethod
    async def isfile(self, url: str) -> bool:
        pass

    @abc.abstractmethod
    async def isdir(self, url: str) -> bool:
        pass

    @abc.abstractmethod
    async def remove(self, url: str) -> None:
        pass

    @abc.abstractmethod
    async def rmtree(self, url: str) -> None:
        pass

    async def touch(self, url: str) -> None:
        async with await self.create(url):
            pass

    async def close(self) -> None:
        pass

    async def __aenter__(self: AsyncFSType) -> AsyncFSType:
        return self

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_val: Optional[BaseException],
                        exc_tb: Optional[TracebackType]) -> None:
        await self.close()


class LocalStatFileStatus(FileStatus):
    def __init__(self, stat_result):
        self._stat_result = stat_result
        self._items = None

    async def size(self) -> int:
        return self._stat_result.st_size

    async def __getitem__(self, key: str) -> Any:
        raise KeyError(key)


class LocalFileListEntry(FileListEntry):
    def __init__(self, thread_pool, base_url, entry):
        assert '/' not in entry.name
        self._thread_pool = thread_pool
        if not base_url.endswith('/'):
            base_url = f'{base_url}/'
        self._base_url = base_url
        self._entry = entry
        self._status = None

    def name(self) -> str:
        return self._entry.name

    async def url(self) -> str:
        trailing_slash = "/" if await self.is_dir() else ""
        return f'{self._base_url}{self._entry.name}{trailing_slash}'

    async def is_file(self) -> bool:
        return not await self.is_dir()

    async def is_dir(self) -> bool:
        return await blocking_to_async(self._thread_pool, self._entry.is_dir)

    async def status(self) -> LocalStatFileStatus:
        if self._status is None:
            if await self.is_dir():
                raise ValueError("directory has no file status")
            self._status = LocalStatFileStatus(await blocking_to_async(self._thread_pool, self._entry.stat))
        return self._status


class LocalAsyncFS(AsyncFS):
    def __init__(self, thread_pool: ThreadPoolExecutor, max_workers=None):
        if not thread_pool:
            thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self._thread_pool = thread_pool

    def schemes(self) -> Set[str]:
        return {'file'}

    @staticmethod
    def _get_path(url):
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme and parsed.scheme != 'file':
            raise ValueError(f"invalid scheme, expected file: {parsed.scheme}")
        return parsed.path

    async def open(self, url: str) -> ReadableStream:
        return blocking_readable_stream_to_async(self._thread_pool, cast(BinaryIO, open(self._get_path(url), 'rb')))

    async def create(self, url: str) -> WritableStream:
        return blocking_writable_stream_to_async(self._thread_pool, cast(BinaryIO, open(self._get_path(url), 'wb')))

    async def statfile(self, url: str) -> LocalStatFileStatus:
        path = self._get_path(url)
        stat_result = await blocking_to_async(self._thread_pool, os.stat, path)
        if stat.S_ISDIR(stat_result.st_mode):
            raise FileNotFoundError(f'is directory: {url}')
        return LocalStatFileStatus(stat_result)

    async def _listfiles_recursive(self, url: str) -> AsyncIterator[FileListEntry]:
        async for file in self._listfiles_flat(url):
            if await file.is_file():
                yield file
            else:
                async for subfile in self._listfiles_recursive(await file.url()):
                    yield subfile

    async def _listfiles_flat(self, url: str) -> AsyncIterator[FileListEntry]:
        path = self._get_path(url)
        with await blocking_to_async(self._thread_pool, os.scandir, path) as it:
            for entry in it:
                yield LocalFileListEntry(self._thread_pool, url, entry)

    def listfiles(self, url: str, recursive: bool = False) -> AsyncIterator[FileListEntry]:
        if recursive:
            return self._listfiles_recursive(url)
        return self._listfiles_flat(url)

    async def mkdir(self, url: str) -> None:
        path = self._get_path(url)
        os.mkdir(path)

    async def isfile(self, url: str) -> bool:
        path = self._get_path(url)
        return await blocking_to_async(self._thread_pool, os.path.isfile, path)

    async def isdir(self, url: str) -> bool:
        path = self._get_path(url)
        return await blocking_to_async(self._thread_pool, os.path.isdir, path)

    async def remove(self, url: str) -> None:
        path = self._get_path(url)
        return os.remove(path)

    async def rmtree(self, url: str) -> None:
        path = self._get_path(url)
        await blocking_to_async(self._thread_pool, shutil.rmtree, path)


class Transfer:
    def __init__(src:  Union[str, List[str]], dest: str, *, dest_is_directory=None):
        if dest_is_directory is False and isinstance(src, list):
            raise NotDirectoryError(dest)

        self.src = src
        self.dest = dest
        self.dest_is_directory = dest_is_directory


class SourceCopier:
    def __init__(self, router_fs, src, dest, dest_is_directory, stat_dest_task):
        self.router_fs = router_fs
        self.src = src
        self.dest = dest
        self.dest_is_directory = dest_is_directory
        self.stat_dest_task = stat_dest_task

        self.src_is_file = None
        self.src_is_dir = None

        self.pending = 2
        self.barrier = asyncio.Event()

    async def release_barrier(self):
        self.pending -= 1
        if self.pending == 0:
            self.barrier.set()

    async def wait_barrier(self):
        self.release_barrier()
        await self.barrier.wait()

    async def _copy_file(self, srcfile, destfile):
        async with await self.router_fs.open(srcfile) as srcf:
            try:
                destf = await self.router_fs.create(destfile)
            except FileNotFoundError:
                await self.router_fs.makedirs(os.dirname(destfile), exists_ok=True)
                destf = await self.router_fs.create(destfile)

            async with destf:
                while True:
                    b = await srcf.read(Copier.BUFFER_SIZE)
                    if not b:
                        return
                    await destf.write(b)

    async def _full_dest(self):
        try:
            dest_exists, dest_type = await self.stat_dest_task
        except:
            # if the dest is malformed, the driver will raise
            return None

        dest = self.dest
        if self.dest_is_directory
                or (self.dest_is_directory is None
                    and dest_exists
                    and dest_type == AsyncFS.DIR):
            return url_join(dest, url_basename(src))
        else:
            return dest

    async def copy_as_file(self):
        src = self.src.rstrip('/')

        try:
            src_status = await self.router_fs.statfile(src)
        except FileNotFoundError:
            self.src_is_file = False
            self.release_barrier()
            return

        self.src_is_file = True

        if src.endswith('/'):
            # caller will raise NotADirectoryError
            self.release_barrier()
            return

        self.wait_barrier()

        if self.src_is_dir:
            # caller will raise FileAndDirectoryError
            return

        full_dest = self._full_dest(src)
        if not full_dest:
            # if the dest was malformed, driver will raise an error
            return
        await self._copy_file(src, full_dest)

    async def copy_as_dir(self):
        src = self.src
        if not src.endswith('/'):
            src = src + '/'

        try:
            srcfiles = await self.router_fs.listfiles(src, recursive=True)
        except FileNotFoundError:
            self.src_is_dir = False
            self.release_barrier()
            return

        self.src_is_dir = True
        self.wait_barrier()

        if self.src_is_file:
            # caller will raise FileAndDirectoryError
            return

        full_dest = self._full_dest(src)
        if full_dest is None:
            # if the dest was malformed, driver will raise an error
            return

        async for srcfile in srcfiles:
            assert srcfile.startswith(src)

            # skip files with empty names
            if srcfile.endswith('/'):
                continue

            relsrcfile = srcfile[len(src):]
            assert not relsrcfile.startswith('/')

            # this should be asynchronous
            await copy_file(srcfile, url_join(dest, relsrcfile))

    async def copy(self):
        await asyncio.gather(
            self.copy_as_file(),
            self.copy_as_dir())

        assert self.pending == 0
        assert self.src_is_file is not None
        assert self.src_is_dir is not None

        if self.src_is_file and self.src_is_dir:
            raise FileAndDirectoryError(self.src)
        if self.src_is_file and self.src.endswith('/'):
            raise NotDirectoryError(self.src)

        if not self.src_is_file and not self.src_is_dir:
            raise FileNotFoundError(self.src)


class Copier:
    BUFFER_SIZE = 8192

    def __init__(self, router_fs):
        self.router_fs = router_fs

    async def _stat_dest(self):
        dest = self.dest

        if dest.endswith('/'):
            dest_as_file = dest.rstrip('/')
            dest_as_dir = dest
        else:
            dest_as_file = dest
            dest_as_dir = dest + '/'

        [is_file, is_dir] = await asyncio.gather(
            self.router_fs.isfile(dest_as_file)
            self.router_fs.isdir(dest_as_dir))

        dest_type = None
        if is_file:
            if is_dir:
                raise FileAndDirectoryError()
            dest_exists = True
            dest_type = AsyncFS.FILE
        else:
            if is_dir:
                dest_exists = True
                dest_type = AsyncFS.DIR
            else:
                dest_exists = False

        if (dest_exists
            and dest_type == AsyncFS.FILE
            and (self.dest_is_directory
                 or isinstance(self.src, list)
                 or dest.endswith('/'))):
            raise NotADirectoryError(dest)

        return dest_exists, dest_type

    async def copy_source(self, stat_dest_task, src):
        src_copier = SourceCopier(self.router_fs, src, self.dest, self.stat_dest_task)
        src_copier.copy()

    async def _copy1(transfer: Transfer):
        src = trasnfer.src
        dest = transfer.dest
        dest_is_directory = transfer.dest_is_directory

        stat_dest_task = asyncio.create_task(self._stat_dest)

        if isinstance(src, list):
            aws = [stat_dest_task]
            for s in src:
                aws.append(self.copy_source(stat_dest_task, s))
            await asyncio.gather(*aws)
        else:
            await asyncio.gather(
                stat_desk_task,
                self.copy_source(stat_desk_task, src))

    async def copy(transfers: List[Transfer]):
        await asyncio.gather(*[
            self._copy1(transfer)
            for transfer in transfers])


class RouterAsyncFS(AsyncFS):
    def __init__(self, default_scheme: Optional[str], filesystems: List[AsyncFS]):
        scheme_fs = {}
        schemes = set()
        for fs in filesystems:
            for scheme in fs.schemes():
                if scheme not in schemes:
                    scheme_fs[scheme] = fs
                    schemes.add(scheme)

        if default_scheme not in schemes:
            raise ValueError(f'default scheme {default_scheme} not in set of schemes: {", ".join(schemes)}')

        self._default_scheme = default_scheme
        self._filesystems = filesystems
        self._schemes = schemes
        self._scheme_fs = scheme_fs

    def schemes(self) -> Set[str]:
        return self._schemes

    def _get_fs(self, url):
        parsed = urllib.parse.urlparse(url)
        if not parsed.scheme:
            if self._default_scheme:
                parsed = parsed._replace(scheme=self._default_scheme)
                url = urllib.parse.urlunparse(parsed)
            else:
                raise ValueError(f"no default scheme and URL has no scheme: {url}")

        fs = self._scheme_fs.get(parsed.scheme)
        if fs is None:
            raise ValueError(f"unknown scheme: {parsed.scheme}")

        return fs

    async def open(self, url: str) -> ReadableStream:
        fs = self._get_fs(url)
        return await fs.open(url)

    async def create(self, url: str) -> WritableStream:
        fs = self._get_fs(url)
        return await fs.create(url)

    async def statfile(self, url: str) -> FileStatus:
        fs = self._get_fs(url)
        return await fs.statfile(url)

    def listfiles(self, url: str, recursive: bool = False) -> AsyncIterator[FileListEntry]:
        fs = self._get_fs(url)
        return fs.listfiles(url, recursive)

    async def mkdir(self, url: str) -> None:
        fs = self._get_fs(url)
        return await fs.mkdir(url)

    async def isfile(self, url: str) -> bool:
        fs = self._get_fs(url)
        return await fs.isfile(url)

    async def isdir(self, url: str) -> bool:
        fs = self._get_fs(url)
        return await fs.isdir(url)

    async def remove(self, url: str) -> None:
        fs = self._get_fs(url)
        return await fs.remove(url)

    async def rmtree(self, url: str) -> None:
        fs = self._get_fs(url)
        return await fs.rmtree(url)

    async def close(self) -> None:
        for fs in self._filesystems:
            await fs.close()

    @staticmethod
    def copy(transfers: List[Transfer]):
        copier = Copier(self)
        copier.execute(transfers)

