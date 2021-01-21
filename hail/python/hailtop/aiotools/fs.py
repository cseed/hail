from typing import TypeVar, Any, Optional, List, Type, BinaryIO, cast, Set, AsyncIterator, Union
from types import TracebackType
import abc
import os
import os.path
import stat
import shutil
import asyncio
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
    def url_maybe_trailing_slash(self) -> str:
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
    FILE = 'file'
    DIR = 'dir'

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
    async def makedirs(self, url: str, exist_ok: bool = False) -> None:
        pass

    @abc.abstractmethod
    async def statfile(self, url: str) -> FileStatus:
        pass

    @abc.abstractmethod
    async def listfiles(self, url: str, recursive: bool = False) -> AsyncIterator[FileListEntry]:
        pass

    @abc.abstractmethod
    async def staturl(self, url: str) -> str:
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

    def url_maybe_trailing_slash(self) -> str:
        return f'{self._base_url}{self._entry.name}'

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

    # entries has no type hint because the return type of os.scandir
    # appears to be a private type, posix.ScandirIterator.
    # >>> import os
    # >>> entries = os.scandir('.')
    # >>> type(entries)
    # <class 'posix.ScandirIterator'>
    # >>> import posix
    # >>> posix.ScandirIterator
    # Traceback (most recent call last):
    #   File "<stdin>", line 1, in <module>
    # AttributeError: module 'posix' has no attribute 'ScandirIterator'
    async def _listfiles_recursive(self, url: str, entries) -> AsyncIterator[FileListEntry]:
        async for file in self._listfiles_flat(url, entries):
            if await file.is_file():
                yield file
            else:
                new_url = await file.url()
                new_path = self._get_path(new_url)
                new_entries = await blocking_to_async(self._thread_pool, os.scandir, new_path)
                async for subfile in self._listfiles_recursive(new_url, new_entries):
                    yield subfile

    async def _listfiles_flat(self, url: str, entries) -> AsyncIterator[FileListEntry]:
        with entries:
            for entry in entries:
                yield LocalFileListEntry(self._thread_pool, url, entry)

    async def listfiles(self, url: str, recursive: bool = False) -> AsyncIterator[FileListEntry]:
        path = self._get_path(url)
        entries = await blocking_to_async(self._thread_pool, os.scandir, path)
        if recursive:
            return self._listfiles_recursive(url, entries)
        return self._listfiles_flat(url, entries)

    async def staturl(self, url: str) -> str:
        path = self._get_path(url)
        stat_result = await blocking_to_async(self._thread_pool, os.stat, path)
        if stat.S_ISDIR(stat_result.st_mode):
            return AsyncFS.DIR
        return AsyncFS.FILE

    async def mkdir(self, url: str) -> None:
        path = self._get_path(url)
        await blocking_to_async(self._thread_pool, os.mkdir, path)

    async def makedirs(self, url: str, exist_ok: bool = False) -> None:
        path = self._get_path(url)
        await blocking_to_async(self._thread_pool, os.makedirs, path, exist_ok=exist_ok)

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


class FileAndDirectoryError(Exception):
    pass


class Transfer:
    TARGET_DIR = 'target_dir'
    TARGET_FILE = 'target_file'
    INFER_TARGET = 'infer_target'

    def __init__(self, src: Union[str, List[str]], dest: str, *, treat_dest_as: str = INFER_TARGET):
        if treat_dest_as not in (Transfer.TARGET_DIR, Transfer.TARGET_FILE, Transfer.INFER_TARGET):
            raise ValueError(f'treat_dest_as invalid: {treat_dest_as}')

        if treat_dest_as == Transfer.TARGET_FILE and isinstance(src, list):
            raise NotADirectoryError(dest)

        self.src = src
        self.dest = dest
        self.treat_dest_as = treat_dest_as


class SourceCopier:
    '''This class implements copy from a single source.  In general, a
    transfer will have multiple sources, and a SourceCopier will be
    created for each source.
    '''

    def __init__(self, router_fs: 'RouterAsyncFS', src: str, dest: str, treat_dest_as: str, dest_type_task):
        self.router_fs = router_fs
        self.src = src
        self.dest = dest
        self.treat_dest_as = treat_dest_as
        self.dest_type_task = dest_type_task

        self.src_is_file = None
        self.src_is_dir = None

        self.pending = 2
        self.barrier = asyncio.Event()

    async def release_barrier(self):
        self.pending -= 1
        if self.pending == 0:
            self.barrier.set()

    async def release_barrier_and_wait(self):
        await self.release_barrier()
        await self.barrier.wait()

    async def _copy_file(self, srcfile: str, destfile: str) -> None:
        assert not destfile.endswith('/')

        async with await self.router_fs.open(srcfile) as srcf:
            try:
                destf = await self.router_fs.create(destfile)
            except FileNotFoundError:
                await self.router_fs.makedirs(os.path.dirname(destfile), exist_ok=True)
                destf = await self.router_fs.create(destfile)

            async with destf:
                while True:
                    b = await srcf.read(Copier.BUFFER_SIZE)
                    if not b:
                        return
                    await destf.write(b)

    async def _full_dest(self):
        dest_type = await self.dest_type_task

        if (self.treat_dest_as == Transfer.TARGET_DIR
                or self.dest.endswith('/')
                or (self.treat_dest_as == Transfer.INFER_TARGET
                    and dest_type == AsyncFS.DIR)):
            if dest_type is None:
                raise FileNotFoundError(self.dest)
            if dest_type == AsyncFS.FILE:
                raise NotADirectoryError(self.dest)
            assert dest_type == AsyncFS.DIR
            # We know dest is a dir, but we're copying to
            # dest/basename(src), and we don't know its type.
            return url_join(self.dest, url_basename(self.src.rstrip('/'))), None

        assert not self.dest.endswith('/')
        return self.dest, dest_type

    async def copy_as_file(self):
        src = self.src
        if src.endswith('/'):
            await self.release_barrier()
            return

        try:
            # currently unused; size will be use to do mutli-part
            # uploads
            await self.router_fs.statfile(src)
        except FileNotFoundError:
            self.src_is_file = False
            await self.release_barrier()
            return

        self.src_is_file = True
        await self.release_barrier_and_wait()

        if self.src_is_dir:
            raise FileAndDirectoryError(self.src)

        full_dest, full_dest_type = await self._full_dest()
        if full_dest_type == AsyncFS.DIR:
            raise IsADirectoryError(full_dest)

        await self._copy_file(src, full_dest)

    async def copy_as_dir(self):
        src = self.src
        if not src.endswith('/'):
            src = src + '/'

        try:
            srcentries = await self.router_fs.listfiles(src, recursive=True)
        except (NotADirectoryError, FileNotFoundError):
            self.src_is_dir = False
            await self.release_barrier()
            return

        self.src_is_dir = True
        await self.release_barrier_and_wait()

        if self.src_is_file:
            raise FileAndDirectoryError(self.src)

        full_dest, full_dest_type = await self._full_dest()
        if full_dest_type == AsyncFS.FILE:
            raise NotADirectoryError(full_dest)

        async for srcentry in srcentries:
            srcfile = srcentry.url_maybe_trailing_slash()
            assert srcfile.startswith(src)

            # skip files with empty names
            if srcfile.endswith('/'):
                continue

            relsrcfile = srcfile[len(src):]
            assert not relsrcfile.startswith('/')

            await self._copy_file(srcfile, url_join(full_dest, relsrcfile))

    async def copy(self):
        # gather with return_exceptions=True to make copy
        # deterministic with respect to exceptions
        results = await asyncio.gather(
            self.copy_as_file(), self.copy_as_dir(),
            return_exceptions=True)

        assert self.pending == 0
        assert (self.src_is_file is None) == self.src.endswith('/')
        assert self.src_is_dir is not None

        if (self.src_is_file is False or self.src.endswith('/')) and not self.src_is_dir:
            raise FileNotFoundError(self.src)
        for result in results:
            if isinstance(result, Exception):
                raise result


class Copier:
    '''
    This class implements copy for a list of transfers.
    '''
    
    BUFFER_SIZE = 8192

    def __init__(self, router_fs):
        self.router_fs = router_fs

    async def _dest_type(self, transfer):
        '''Return the (real or assumed) type of `dest`.

        If the transfer assumes the type of `dest`, return that rather
        than the real type.  A return value of `None` mean `dest` does
        not exist.
        '''
        if (transfer.treat_dest_as == Transfer.TARGET_DIR
                or isinstance(transfer.src, list)
                or transfer.dest.endswith('/')):
            return AsyncFS.DIR

        if transfer.treat_dest_as == Transfer.TARGET_FILE:
            return AsyncFS.FILE

        assert not transfer.dest.endswith('/')
        try:
            dest_type = await self.router_fs.staturl(transfer.dest)
        except FileNotFoundError:
            dest_type = None

        return dest_type

    async def copy_source(self, transfer, src, dest_type_task):
        src_copier = SourceCopier(self.router_fs, src, transfer.dest, transfer.treat_dest_as, dest_type_task)
        await src_copier.copy()

    async def _copy_one_transfer(self, transfer: Transfer):
        dest_type_task = asyncio.create_task(self._dest_type(transfer))
        dest_type_task_awaited = False

        try:
            src = transfer.src
            if isinstance(src, list):
                if transfer.treat_dest_as == Transfer.TARGET_FILE:
                    raise NotADirectoryError(transfer.dest)

                for s in src:
                    await self.copy_source(transfer, s, dest_type_task)
            else:
                await self.copy_source(transfer, src, dest_type_task)

            # raise potential exception
            dest_type_task_awaited = True
            await dest_type_task
        finally:
            if not dest_type_task_awaited:
                # retrieve dest_type_task exception to avoid
                # "Task exception was never retrieved" errors
                try:
                    dest_type_task_awaited = True
                    await dest_type_task
                except:
                    pass

    async def copy(self, transfer: Union[Transfer, List[Transfer]]):
        if isinstance(transfer, Transfer):
            await self._copy_one_transfer(transfer)
            return

        for t in transfer:
            await self._copy_one_transfer(t)


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

    async def listfiles(self, url: str, recursive: bool = False) -> AsyncIterator[FileListEntry]:
        fs = self._get_fs(url)
        return await fs.listfiles(url, recursive)

    async def staturl(self, url: str) -> str:
        fs = self._get_fs(url)
        return await fs.staturl(url)

    async def mkdir(self, url: str) -> None:
        fs = self._get_fs(url)
        return await fs.mkdir(url)

    async def makedirs(self, url: str, exist_ok: bool = False) -> None:
        fs = self._get_fs(url)
        return await fs.makedirs(url, exist_ok=exist_ok)

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

    async def copy(self, transfer: Union[Transfer, List[Transfer]]):
        copier = Copier(self)
        await copier.copy(transfer)
