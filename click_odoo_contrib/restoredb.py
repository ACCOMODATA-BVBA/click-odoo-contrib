#!/usr/bin/env python
# Copyright 2019 ACSONE SA/NV (<http://acsone.eu>)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html).

import os
import shutil
import subprocess

import click
import click_odoo
import psycopg2
from click_odoo import OdooEnvironment, odoo

import fsspec

from ._dbutils import db_exists, db_management_enabled, reset_config_parameters
from ._storage_config import (
    DUMP_SQL_FILENAME,
    DBDUMP_FILENAME,
    FILESTORE_DIRNAME,
    MANIFEST_FILENAME,
    FS_ATTACHMENT_DIRNAME,
    get_target_filehandle,
)

import logging


_logger = logging.getLogger(__name__)


def _extract_zip_fileh(dbname, source_zip_fh):
    zip_fs = fsspec.filesystem("zip", fo=source_zip_fh)
    for root, dirs, files in zip_fs.walk("/"):
        for filename in files:
            root_stripped = root.strip("/")
            item_name = f"{root_stripped}/{filename}" if root_stripped else filename

            with zip_fs.open(item_name, "rb") as zip_item_fh:
                _restore_item_from_fh(item_name, zip_item_fh, dbname)


def _restore_item_from_fh(archive_filename, file_h, dbname):
    if archive_filename == DUMP_SQL_FILENAME:
        print(f"Restoring dump")
        _restore_psql_from_fileh(archive_filename, file_h, dbname)
    elif ( 
        archive_filename.startswith(FILESTORE_DIRNAME + "/")
        or 
        archive_filename.startswith(FS_ATTACHMENT_DIRNAME + "/")
    ):
        _restore_filestore_fileh(archive_filename, file_h, dbname)
    elif archive_filename.startswith(MANIFEST_FILENAME):
        print('Ignoring file "%s"' % archive_filename)
        pass
    else:
        print(f"Unknown item in zipfile: '{archive_filename}'")


def _restore_psql_from_fileh(item_name, file_h, dbname):
    pg_args = [
        "psql",
        "--quiet",
        f"--dbname={dbname}",
    ]
    pg_env = odoo.tools.misc.exec_pg_environ()
    try:
        psql_popen = subprocess.Popen(
            pg_args,
            env=pg_env,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            while True:
                chunk = file_h.read(65536)
                if not chunk:
                    break
                psql_popen.stdin.write(chunk)
        finally:
            psql_popen.stdin.close()

        returncode = psql_popen.wait()
        if returncode != 0:
            raise Exception(f"psql exited with return code {returncode}")
    except Exception as e:
        _logger.exception("Unexpected error during restore")
        raise click.ClickException("Couldn't restore database") from e


def _restore_filestore_fileh(archive_filename, file_h, dbname):
    out_fh = get_target_filehandle(archive_filename, dbname)
    try:
        shutil.copyfileobj(file_h, out_fh)
    finally:
        out_fh.close()


def _restore_from_source(dbname, source, copy, neutralize):
    odoo.service.db._create_empty_database(dbname)
    with fsspec.open(source, mode="rb") as url_fileh:
        _extract_zip_fileh(dbname, source_zip_fh=url_fileh)

    if copy:
        # if it's a copy of a database, force generation of a new dbuuid
        reset_config_parameters(dbname)
    with OdooEnvironment(dbname) as env:
        if neutralize and odoo.release.version_info >= (16, 0):
            odoo.modules.neutralize.neutralize_database(env.cr)

        if odoo.tools.config["unaccent"]:
            try:
                with env.cr.savepoint():
                    env.cr.execute("CREATE EXTENSION unaccent")
            except psycopg2.Error:
                _logger.exception("Exception while creating extension unaccent")
    odoo.sql_db.close_db(dbname)


def _restore_from_folder(dbname, backup, copy=True, jobs=1, neutralize=False):
    manifest_file_path = os.path.join(backup, MANIFEST_FILENAME)
    dbdump_file_path = os.path.join(backup, DBDUMP_FILENAME)
    filestore_dir_path = os.path.join(backup, FILESTORE_DIRNAME)
    if not os.path.exists(manifest_file_path) or not os.path.exists(dbdump_file_path):
        msg = (
            "{} is not folder backup created by the backupdb command. "
            "{} and {} files are missing.".format(
                backup, MANIFEST_FILENAME, DBDUMP_FILENAME
            )
        )
        raise click.ClickException(msg)

    odoo.service.db._create_empty_database(dbname)
    pg_args = ["--jobs", str(jobs), "--dbname", dbname, "--no-owner", dbdump_file_path]
    pg_env = odoo.tools.misc.exec_pg_environ()
    r = subprocess.run(
        ["pg_restore", *pg_args],
        env=pg_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    if r.returncode != 0:
        raise click.ClickException("Couldn't restore database")
    if copy:
        # if it's a copy of a database, force generation of a new dbuuid
        reset_config_parameters(dbname)
    with OdooEnvironment(dbname) as env:
        if neutralize and odoo.release.version_info >= (16, 0):
            odoo.modules.neutralize.neutralize_database(env.cr)
        if os.path.exists(filestore_dir_path):
            filestore_dest = env["ir.attachment"]._filestore()
            shutil.move(filestore_dir_path, filestore_dest)

        if odoo.tools.config["unaccent"]:
            try:
                with env.cr.savepoint():
                    env.cr.execute("CREATE EXTENSION unaccent")
            except psycopg2.Error:
                pass
    odoo.sql_db.close_db(dbname)


@click.command()
@click_odoo.env_options(
    default_log_level="warn", with_database=False, with_rollback=False
)
@click.option(
    "--copy/--move",
    default=True,
    help=(
        "This database is a copy.\nIn order "
        "to avoid conflicts between databases, Odoo needs to know if this"
        "database was moved or copied. If you don't know, set is a copy."
    ),
)
@click.option(
    "--force",
    is_flag=True,
    show_default=True,
    help=(
        "Don't report error if destination database already exists. If "
        "force and destination database exists, it will be dropped before "
        "restore."
    ),
)
@click.option(
    "--neutralize",
    is_flag=True,
    show_default=True,
    help=(
        "Neutralize the database after restore. This will disable scheduled actions, "
        "outgoing emails, and sets other external providers in test mode. "
        "This works only in odoo 16.0 and above."
    ),
)
@click.option(
    "--jobs",
    help=(
        "Uses this many parallel jobs to restore. Only used to "
        "restore folder format backup."
    ),
    type=int,
    default=1,
)
@click.argument("dbname", nargs=1)
@click.argument(
    "source",
    nargs=1,
)
def main(env, dbname, source, copy, force, neutralize, jobs):
    """Restore an Odoo database backup.

    This script allows you to restore databses created by using the Odoo
    web interface or the backupdb script. This
    avoids timeout and file size limitation problems when
    databases are too large.
    """
    if db_exists(dbname):
        msg = "Destination database already exists: {}".format(dbname)
        if not force:
            raise click.ClickException(msg)
        msg = "{}, dropping it as requested.".format(msg)
        click.echo(click.style(msg, fg="yellow"))
        with db_management_enabled():
            odoo.service.db.exp_drop(dbname)
    if neutralize and odoo.release.version_info < (16, 0):
        raise click.ClickException(
            "--neutralize option is only available in odoo 16.0 and above"
        )
    if os.path.isdir(source):
        _restore_from_folder(dbname, source, copy, jobs, neutralize)
    elif source:
        _restore_from_source(dbname, source, copy, neutralize)
    else:
        raise click.ClickException(
            "SOURCE argument missing"
        )
        
