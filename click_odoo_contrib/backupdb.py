#!/usr/bin/env python
# Copyright 2018 ACSONE SA/NV (<http://acsone.eu>)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html).

import json
import os
import shutil
import subprocess
import fsspec

# Monkey patch s3fs to fix InvalidPartOrder during concurrent uploads
try:
    import s3fs.core
    _original_s3fs_commit = s3fs.core.S3File.commit
    def _patched_s3fs_commit(self):
        if hasattr(self, "parts") and isinstance(self.parts, list):
            try:
                self.parts.sort(key=lambda x: x.get("PartNumber", 0))
            except Exception:
                pass
        return _original_s3fs_commit(self)
    s3fs.core.S3File.commit = _patched_s3fs_commit
except ImportError:
    pass

import click
import click_odoo
from click_odoo import odoo

from ._backup import backup
from ._dbutils import db_exists, db_management_enabled, pg_connect

# Layout for files & folders inside the ZIP archive:
MANIFEST_FILENAME = "manifest.json"
DBDUMP_FILENAME = "db.dump"
FILESTORE_DIRNAME = "filestore"
FS_ATTACHMENT_DIRNAME = "fs_attachment"

# reading configration from odoo.conf:
FS_STORAGE_KEY= 'fs_storage.'
FS_STORAGE_BACKUP_ENTRY = 'odoo_backups'

fsspec_filesystems = {}

# default chunk size for upload with streaming backup
FS_WRITE_CHUNK_SIZE = 4 * 1024 * 1024


def _read_fs_storage_config():
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

        fsspec_filesystems.update(
            {
                storage_code: (fs, directory_path),
            }
        )


def _get_fsspec_filesystem(storage_code):
    if storage_code not in fsspec_filesystems:
        raise Exception("Storage code %s not configured in odoo config", storage_code)
    return fsspec_filesystems[storage_code]


def _dump_db_command(dbname, backup):
    cmd = ["pg_dump", "--no-owner", dbname]
    env = odoo.tools.misc.exec_pg_environ()
    filename = "dump.sql"
    if backup.format in {"dump", "folder"}:
        cmd.insert(-1, "--format=c")
        filename = DBDUMP_FILENAME
    return cmd, env, filename


def _dump_db(dbname, backup):
    cmd, env, filename = _dump_db_command(dbname, backup)
    if backup.format in ("stream-zip","fsspec-zip"):
        backup.add_dump_command(cmd, env, filename)
    else:
        stdout = subprocess.Popen(
            cmd, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE
        ).stdout
        backup.write(stdout, filename)


def _get_filestore_file_list(cr, dbname, minimal=False):
    if minimal:
        qry = (
            "SELECT DISTINCT store_fname "
            "FROM ir_attachment "
            "WHERE create_uid IN (1, 2) "
            "   AND store_fname IS NOT NULL "
            "   AND ("
            "       res_model IN ("
            "           'ir.ui.view',"
            "           'ir.ui.menu',"
            "           'res.company',"
            "           'res.lang' )"
            "          OR res_model IS NULL "
            r"          OR name ILIKE '%assets%'"
            "   )"
        )
    else:
        qry = (
            "SELECT DISTINCT store_fname "
            "FROM ir_attachment "
            "WHERE store_fname IS NOT NULL"
        )
    with pg_connect(dbname) as cr:
        cr.execute(qry)
        result = [item[0] for item in cr.fetchall()]
    return result


def _create_manifest(cr, dbname, backup):
    manifest = odoo.service.db.dump_db_manifest(cr)
    backup.add_data(json.dumps(manifest, indent=4).encode("utf-8"), MANIFEST_FILENAME)


def _backup_filestore(cr, dbname, backup, minimal):
    filestore_path = odoo.tools.config.filestore(dbname)

    for store_fname in _get_filestore_file_list(cr, dbname, minimal):
        if "://" in store_fname:
            # fs_attachment based attachment
            storage_code, _ignored, fname = store_fname.partition("://")
            archive_name = os.path.join(
                FS_ATTACHMENT_DIRNAME,
                storage_code,
                fname,
            )
            fs, directory_path = _get_fsspec_filesystem(storage_code)
            path = os.path.normpath(os.path.join(directory_path, fname))
            with fs.open(path, mode="rb") as fh:
                backup.add_fileh(fh, archive_name)
        else:
            # classic filestore attachment
            path = os.path.normpath(os.path.join(filestore_path, store_fname))
            archive_name = os.path.join(
                FILESTORE_DIRNAME,
                os.path.normpath(store_fname),
            )
            if os.path.isfile(path):
                backup.addfile(path, archive_name)


@click.command()
@click_odoo.env_options(
    default_log_level="warn", with_database=False, with_rollback=False
)
@click.option(
    "--force",
    is_flag=True,
    show_default=True,
    help="Don't report error if destination file/folder already exists.",
)
@click.option(
    "--if-exists", is_flag=True, help="Don't report error if database does not exist."
)
@click.option(
    "--format",
    type=click.Choice(["zip", "dump", "folder", "stream-zip", "fsspec-zip"]),
    default="zip",
    show_default=True,
    help="Output format",
)
@click.option(
    "--filestore",
    "filestore",
    flag_value="full",
    default="full",
    help="Include full filestore in backup",
)
@click.option(
    "--no-filestore",
    "filestore",
    flag_value="none",
    help="Do not include filestore in backup",
)
@click.option(
    "--filestore-minimal",
    "filestore",
    flag_value="minimal",
    help="Include only minimal filestore",
)

@click.argument("dbname", nargs=1)
@click.argument("dest", nargs=1, required=1)
def main(
    env,
    dbname,
    dest,
    force,
    if_exists,
    format,
    filestore,
):
    """Create an Odoo database backup from an existing one.

    This script dumps the database using pg_dump.
    It also copies the filestore.

    Unlike Odoo, this script allows you to make a backup of a
    database without going through the web interface. This
    avoids timeout and file size limitation problems when
    databases are too large.

    It also allows you to make a backup directly to a directory.
    This type of backup has the advantage that it reduces
    memory consumption since the files in the filestore are
    directly copied to the target directory as well as the
    database dump.

    Finally this script allows to upload directly to remote storage
    streaming the zip file without having memory or diskspace
    constraints that large backups might introduce.  Choose stream-zip
    as output format for this. Configuration for fsspec is 
    read from odoo.conf

    This script also supports backup from attachments stored using
    the fs_attachment OCA module.  These attachments will be stored
    in the zip file in a folder with the same name as the fs_storage
    name, in the folder "fs_storage/[storage_name]/"

    """
    if not db_exists(dbname):
        msg = "Database does not exist: {}".format(dbname)
        if if_exists:
            click.echo(click.style(msg, fg="yellow"))
            return
        else:
            raise click.ClickException(msg)
    if os.path.exists(dest):
        msg = "Destination already exist: {}".format(dest)
        if not force:
            raise click.ClickException(msg)
        else:
            msg = "\n".join([msg, "Remove {}".format(dest)])
            click.echo(click.style(msg, fg="yellow"))
            if os.path.isfile(dest):
                os.unlink(dest)
            else:
                shutil.rmtree(dest)

    _read_fs_storage_config()

    if format in("fsspec-zip", "stream-zip"):

        fs, directory = _get_fsspec_filesystem(FS_STORAGE_BACKUP_ENTRY)

        backup_fullpath = f'{directory}/{dest}'
        # Run touch to identify access problems early
        fs.touch(backup_fullpath)

    if format == "dump":
        filestore = False
    db = odoo.sql_db.db_connect(dbname)
    try:
        if format == "fsspec-zip":
            fsspec_out = fs.open(
                backup_fullpath, 
                mode='wb', 
                blocksize=FS_WRITE_CHUNK_SIZE,
            )
        else:
            fsspec_out = None

        with backup(
            format, dest, "w", fsspec_out=fsspec_out
        ) as _backup, db.cursor() as cr, db_management_enabled():

            if format != "dump":
                print('Adding manifest')
                _create_manifest(cr, dbname, _backup)
            if filestore == "minimal":
                print('Adding minimal filestore')
                _backup_filestore(cr, dbname, _backup, minimal=True)
            elif filestore == "full":
                print('Adding full filestore')
                _backup_filestore(cr, dbname, _backup, minimal=False)

            print('Adding dump')
            _dump_db(dbname, _backup)
        if format == "stream-zip":
            backup_fileh = _backup.get_file_object()

            with fs.open(
                backup_fullpath, 
                mode='wb', 
                blocksize=FS_WRITE_CHUNK_SIZE
            ) as upload_fileh:
                while True:
                    buffer = backup_fileh.read(FS_WRITE_CHUNK_SIZE)
                    if not buffer:
                        break
                    upload_fileh.write(buffer)
    finally:
        if fsspec_out:
            fsspec_out.close()
        odoo.sql_db.close_db(dbname)


if __name__ == "__main__":  # pragma: no cover
    main()
