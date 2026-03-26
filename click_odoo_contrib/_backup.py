# Copyright 2018 ACSONE SA/NV.
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl).
import os
import shutil
import tempfile
import zipfile
from contextlib import contextmanager
import subprocess

import stream_zip
import datetime
import to_file_like_obj

# default chunk size for generators in streaming backup
DEFAULT_CHUNK_SIZE = 64 * 1024


class CommandReturnException(Exception):
    pass


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

    def add_data(self, buffer, arcname):
        with tempfile.NamedTemporaryFile(mode="w") as f:
            f.write(buffer)
            f.seek(0)
            backup.addfile(f.name, arcname)

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

    def __init__(self, path, mode):
        super().__init__(path, mode)
        os.mkdir(self._path)

    def addtree(self, src, arcname):
        dest = os.path.join(self._path, arcname)
        shutil.copytree(src, dest)

    def addfile(self, filename, arcname):
        shutil.copyfile(filename, os.path.join(self._path, arcname))

    def add_data(self, buffer, arcname):
        with tempfile.NamedTemporaryFile(mode="w") as f:
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


class StreamingZipBackup(ZipBackup):
    format = "stream-zip"
    chunk_size = DEFAULT_CHUNK_SIZE

    def __init__(self, path, mode, chunk_size=None):
        if mode != "w":
            raise NotImplementedError('Only mode "w" is supported here')
        self._path = path
        self._mode = mode
        self._zip_members = []
        if chunk_size is not None:
            self.chunk_size = chunk_size

    def add_data(self, buffer, arcname):
        def _yield_buffer():
            yield buffer

        self._zip_members.append(
            (
                arcname,
                datetime.datetime.now(),
                0o664,
                stream_zip.ZIP_64,
                _yield_buffer(),
            )
        )

    def addfile(self, filename, arcname):
        print(f"Adding file {filename} to zip as {arcname}")

        def _get_file_chunks():
            with open(filename, "rb") as fileh:
                while True:
                    chunk = fileh.read(self.chunk_size)
                    if not chunk:
                        break
                    yield chunk

        statinfo = os.stat(filename)

        self._zip_members.append(
            (
                arcname,
                datetime.datetime.fromtimestamp(statinfo.st_mtime),
                0o664,
                stream_zip.ZIP_64,
                _get_file_chunks(),
            )
        )

    def write(self, stream, arcname):
        raise NotImplementedError()  # pragma: no cover

    def add_dump_command(self, cmd, env, filename):
        def _generator():
            dump = subprocess.Popen(
                cmd, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE
            )

            while True:
                chunk = dump.stdout.read(self.chunk_size)
                if not chunk:
                    dump.stdout.close()
                    dump.wait(timeout=60)
                    if dump.returncode != 0:
                        raise CommandReturnException()
                    if dump.returncode == 0:
                        break
                yield chunk

        self._zip_members.append(
            (
                "dump.sql",
                datetime.datetime.now(),
                0o664,
                stream_zip.ZIP_64,
                _generator(),
            )
        )

    def get_file_object(self):
        return to_file_like_obj.to_file_like_obj(
            stream_zip.stream_zip(self._zip_members)
        )

    def close(self):
        pass

    def delete(self):
        try:
            self.close()
        finally:
            os.unlink(self._path)


BACKUP_FORMAT = {
    ZipBackup.format: ZipBackup,
    DumpBackup.format: DumpBackup,
    FolderBackup.format: FolderBackup,
    StreamingZipBackup.format: StreamingZipBackup,
}


@contextmanager
def backup(format, path, mode):
    backup_class = BACKUP_FORMAT.get(format)
    if not backup_class:  # pragma: no cover
        raise Exception(
            "Format {} not supported. Available formats: {}".format(
                format, "|".join(BACKUP_FORMAT.keys())
            )
        )
    _backup = backup_class(path, mode)
    try:
        yield _backup
        _backup.close()
    except Exception as e:
        _backup.delete()
        raise e
