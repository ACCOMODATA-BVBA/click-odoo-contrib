# Copyright 2018 ACSONE SA/NV.
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl).
import os
import shutil
import tempfile
import zipfile
from contextlib import contextmanager
import subprocess
import fsspec

import datetime

# default chunk size for generators in streaming backup
DEFAULT_CHUNK_SIZE = 1024 * 1024

@contextmanager
def popen_check(*args, **kwargs):
    with subprocess.Popen(*args, **kwargs) as proc:
        yield proc
    proc.stdout.close()
    proc.wait(timeout=60)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, proc.args)


class AbstractBackup:
    """Abstract class with methods to open, read, write, close,
    a backup
    """

    def __init__(self, path, mode):
        """
        :param path: Either the path to the file, or a file-like object, or a folder
                     or ... .
        :param mode: The mode can be either read "r", write "w" or append "a"
        """
        self._path = path
        self._mode = mode

    def addtree(self, src, arcname):
        """Recursively add a directory tree into the backup.
        :param dirname: Directory to copy from
        :param arcname: the root path of copied files into the archive
        """
        raise NotImplementedError()  # pragma: no cover

    def add_data(self, buffer, arcname):
        """Add a data buffer to the backup.

        :param buffer: the buffer containing the data
        :param arcname: the path into the backup
        """
        raise NotImplementedError()  # pragma: no cover

    def addfile(self, filename, arcname):
        """Add a file to the backup.

        :param filename: the path to the souce file
        :param arcname: the path into the backup
        """
        raise NotImplementedError()  # pragma: no cover

    def write(self, stream, arcname):
        """Write a stream into the backup.

        :param arcname: the path into the backup
        """
        raise NotImplementedError()  # pragma: no cover

    def close(self):
        """Close the backup"""
        raise NotImplementedError()  # pragma: no cover

    def delete(self):
        """Delete the backup"""
        raise NotImplementedError()  # pragma: no cover


class ZipBackup(AbstractBackup):
    format = "zip"
    chunk_size = DEFAULT_CHUNK_SIZE

    def __init__(self, path, mode):
        super().__init__(path, mode)
        self._zipFile = zipfile.ZipFile(
            self._path, self._mode, compression=zipfile.ZIP_DEFLATED, allowZip64=True
        )

    def addtree(self, src, arcname):
        len_prefix = len(src) + 1
        for dirpath, _dirnames, filenames in os.walk(src):
            for fname in filenames:
                path = os.path.normpath(os.path.join(dirpath, fname))
                if os.path.isfile(path):
                    _arcname = os.path.join(arcname, path[len_prefix:])
                    self.addfile(path, _arcname)

    def addfile(self, filename, arcname):
        self._zipFile.write(filename, arcname)

    def add_fsspec_file(self, fs, filename, arcname):
        with fs.open(filename, mode="rb") as inputh:
            with tempfile.NamedTemporaryFile(mode="wb") as tempfh:
                while True:
                    chunk = inputh.read(self.chunk_size)
                    if not chunk:
                        break
                    tempfh.write(chunk)

                tempfh.seek(0)
                self.addfile(tempfh.name, arcname)

    def add_data(self, buffer, arcname):
        with tempfile.NamedTemporaryFile(mode="wb") as f:
            f.write(buffer)
            f.seek(0)
            self.addfile(f.name, arcname)

    def write(self, stream, arcname):
        with tempfile.NamedTemporaryFile() as f:
            shutil.copyfileobj(stream, f)
            f.seek(0)
            self._zipFile.write(f.name, arcname)

    def close(self):
        self._zipFile.close()

    def delete(self):
        try:
            self.close()
        finally:
            os.unlink(self._path)


class DumpBackup(AbstractBackup):
    format = "dump"

    def write(self, stream, arcname):
        with open(os.path.join(self._path), "wb") as f:
            shutil.copyfileobj(stream, f)

    def close(self):
        pass

    def delete(self):
        os.remove(self._path)


class FolderBackup(AbstractBackup):
    format = "folder"
    chunk_size = DEFAULT_CHUNK_SIZE

    def __init__(self, path, mode):
        super().__init__(path, mode)
        os.mkdir(self._path)

    def addtree(self, src, arcname):
        dest = os.path.join(self._path, arcname)
        shutil.copytree(src, dest)

    def addfile(self, filename, arcname):
        destination = os.path.join(self._path, arcname)
        dest_folder = os.path.dirname(destination)
        os.makedirs(dest_folder, exist_ok=True)
        shutil.copyfile(filename, destination)

    def add_fsspec_file(self, fs, filename, arcname):
        with fs.open(filename, mode="rb") as inputh:
            with tempfile.NamedTemporaryFile(mode="wb") as tempfh:
                while True:
                    chunk = inputh.read(self.chunk_size)
                    if not chunk:
                        break
                    tempfh.write(chunk)

                tempfh.seek(0)
                self.addfile(tempfh.name, arcname)

    def add_data(self, buffer, arcname):
        with tempfile.NamedTemporaryFile(mode="wb") as f:
            f.write(buffer)
            f.seek(0)
            shutil.copyfile(f.name, os.path.join(self._path, arcname))

    def write(self, stream, arcname):
        with open(os.path.join(self._path, arcname), "wb") as f:
            shutil.copyfileobj(stream, f)

    def close(self):
        pass

    def delete(self):
        shutil.rmtree(self._path)

class QueuedWriter:
    """
    A file-like object that buffers writes in memory and flushes them to an
    underlying file object via a background thread. 
    
    This prevents fast CPU-bound operations (like zip compression) from blocking
    on slow I/O bound operations (like S3 network uploads). It limits memory 
    usage based on total bytes rather than chunk count, which handles thousands 
    of tiny writes efficiently without premature blocking.
    """
    def __init__(self, file_obj, max_bytes=128 * 1024 * 1024):
        import threading
        self.file_obj = file_obj
        self.max_bytes = max_bytes
        
        # Thread synchronization primitives
        self.condition = threading.Condition()
        
        # State
        self.chunks = []
        self.bytes_in_queue = 0
        self._closed = False
        self._exception = None
        self._pos = 0  # To satisfy zipfile's tell() requirement
        
        # Start background writer thread
        self.thread = threading.Thread(target=self._writer_thread, daemon=True)
        self.thread.start()
        
    def _writer_thread(self):
        """Background thread that consumes the queue and writes to the underlying file."""
        try:
            while True:
                with self.condition:
                    # Wait until there is data to write, the writer is closed, or an error occurred
                    while not self.chunks and not self._closed and not self._exception:
                        self.condition.wait()
                    
                    if self._exception:
                        break  # Abort if another thread encountered an error
                    
                    if not self.chunks and self._closed:
                        break  # Clean exit when everything is flushed and closed
                    
                    # Coalesce small chunks into larger ones (up to 8MB). 
                    # Network streams (like s3fs) perform poorly with thousands of tiny write calls.
                    # Grouping them improves CPU and network throughput significantly.
                    buffer = []
                    buffer_len = 0
                    while self.chunks and buffer_len < 8 * 1024 * 1024:
                        chunk = self.chunks.pop(0)
                        buffer.append(chunk)
                        buffer_len += len(chunk)
                        
                    self.bytes_in_queue -= buffer_len
                    # Notify the main thread that space has opened up in the queue
                    self.condition.notify_all()
                    
                # Write outside the lock to allow concurrent enqueueing from the main thread
                chunk_data = b"".join(buffer)
                self.file_obj.write(chunk_data)
                
        except Exception as e:
            # If the network write fails, capture the exception and stop processing.
            # The main thread will raise this exception on its next interaction.
            with self.condition:
                self._exception = e
                self.chunks.clear()
                self.bytes_in_queue = 0
                self.condition.notify_all()

    def write(self, data):
        """Called by the main thread (e.g., zipfile) to queue data for writing."""
        if not data: 
            return 0
            
        data_len = len(data)
        with self.condition:
            if self._exception:
                raise self._exception
                
            # Block the main thread if the queue is full (compressing faster than network upload)
            while self.bytes_in_queue + data_len > self.max_bytes and not self._exception:
                self.condition.wait()
                
            # Check exception again in case it was raised while we were waiting
            if self._exception:
                raise self._exception
                
            self.chunks.append(data)
            self.bytes_in_queue += data_len
            self.condition.notify_all()  # Wake up the writer thread
            
        self._pos += data_len
        return data_len

    def close(self):
        """Flush remaining data and join the background thread."""
        with self.condition:
            if self._closed: 
                return
            self._closed = True
            self.condition.notify_all()  # Ensure thread wakes up to exit
            
        self.thread.join()
        
        # Re-raise any network/writing errors in the main thread
        if self._exception:
            raise self._exception

    def flush(self):
        # We don't forcefully flush the background thread queue here.
        # zipfile calls flush() frequently; blocking here would defeat the queue's purpose.
        if hasattr(self.file_obj, 'flush'):
            self.file_obj.flush()
            
    def tell(self):
        # zipfile strictly requires tell() to work synchronously
        return self._pos

    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class FsspecZipBackup(ZipBackup):
    format = "fsspec-zip"
    chunk_size = 1024 * 1024

    def __init__(self, path, mode, chunk_size=None, fsspec_out=None):
        if mode != "w":
            raise NotImplementedError('Only mode "w" is supported here')

        if fsspec_out is None:
            raise Exception('Cannot use None as output file')

        if chunk_size is not None:
            self.chunk_size = chunk_size
        
        self.queued_out = QueuedWriter(fsspec_out, max_bytes=128 * 1024 * 1024)
        self.zip_fs = fsspec.filesystem("zip",mode=mode, fo=self.queued_out)

    def add_data(self, buffer, arcname):
        with self.zip_fs.open(arcname, 'wb') as out:
            out.write(buffer)

    def addfile(self, filename, arcname):
        self.zip_fs.put(filename, arcname)

    def add_fileh(self, fileh, arcname):
        with self.zip_fs.open(arcname, 'wb', blocksize=self.chunk_size) as out:
            while True:
                buffer = fileh.read(self.chunk_size)
                if not buffer:
                    break
                out.write(buffer)

    def write(self, stream, arcname):
        raise NotImplementedError()  # pragma: no cover

    def add_dump_command(self, cmd, env, arcname='dump.sql'):
        with popen_check(
            cmd, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE
        ) as dump:
            self.add_fileh(dump.stdout, arcname)

    def close(self):
        self.zip_fs.close()
        self.queued_out.close()

    def delete(self):
        raise NotImplementedError




BACKUP_FORMAT = {
    ZipBackup.format: ZipBackup,
    DumpBackup.format: DumpBackup,
    FolderBackup.format: FolderBackup,
    FsspecZipBackup.format: FsspecZipBackup,
}


@contextmanager
def backup(format, path, mode, fsspec_out=None):
    backup_class = BACKUP_FORMAT.get(format)
    if not backup_class:  # pragma: no cover
        raise Exception(
            "Format {} not supported. Available formats: {}".format(
                format, "|".join(BACKUP_FORMAT.keys())
            )
        )
    _backup = backup_class(path, mode, fsspec_out=fsspec_out)
    try:
        yield _backup
        _backup.close()
    except Exception as e:
        _backup.delete()
        raise e
