from click_odoo import odoo
import json
import fsspec
import os


# Layout for files & folders inside the ZIP archive:
MANIFEST_FILENAME = "manifest.json"
DBDUMP_FILENAME = "db.dump"
DUMP_SQL_FILENAME = "dump.sql"
FILESTORE_DIRNAME = "filestore"
FS_ATTACHMENT_DIRNAME = "fs_attachment"

# reading configration from odoo.conf:
FS_STORAGE_KEY = "fs_storage."

# Read the destination for backups also from odoo config
FS_STORAGE_BACKUP_ENTRY = "odoo_backups"


def _load_config():
    config = {
        'local': ( fsspec.filesystem('local'), None)
    }
    for item, vals in odoo.tools.config.misc.items():
        if not item.startswith(FS_STORAGE_KEY):
            continue
        _, storage_code = item.split(".")
        try:
            protocol = vals["protocol"]
            options = json.loads(vals["options"])
            directory_path = vals["directory_path"]
        except Exception as ex:
            print(
                "Failed to read fs_storage config, "
                'expecting keys "protocol", "options" and "directory_path"'
            )
            raise Exception from ex
        fs = fsspec.filesystem(protocol, **options)

        config.update(
            {
                storage_code: (fs, directory_path),
            }
        )
    return config


_fsspec_filesystems = _load_config()


def get_fsspec_filesystem(storage_code):
    if storage_code not in _fsspec_filesystems:
        raise Exception("Storage code %s not configured in odoo config", storage_code)
    return _fsspec_filesystems[storage_code]

def get_target_filehandle(archive_name, dbname):
    if archive_name.startswith(FILESTORE_DIRNAME + '/'):
        filestore_folder = odoo.tools.config.filestore(dbname)
        full_path = os.path.join(
            filestore_folder, archive_name.replace(f"{FILESTORE_DIRNAME}/", "")
        )
        # return file like object from fsspec:
        return fsspec.open(
            urlpath=full_path,
            mode='wb',
            protocol='dir',
            fo=filestore_folder,
            auto_mkdir=True
        ).open()
    elif archive_name.startswith(FS_ATTACHMENT_DIRNAME + '/'):
        # backup files stored in a remote FS get a filename with the following
        # pattern: [FS_ATTACHMENT_DIRNAME]/[STORAGE_CODE]/[file_path]
        _ignored, storage_code, *file_path = archive_name.lstrip('/').split('/')
        fs, directory_path = get_fsspec_filesystem(storage_code)
        if directory_path:
            file_path = [directory_path] + file_path
        print(f'Restoring archive file {archive_name} to {storage_code} -> {"/".join(file_path)}')
        return fs.open('/'.join(file_path), mode='wb')
