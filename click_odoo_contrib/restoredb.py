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


from ._dbutils import db_exists, db_management_enabled, reset_config_parameters
from .backupdb import DBDUMP_FILENAME, FILESTORE_DIRNAME, MANIFEST_FILENAME
from ._download_utils import url_chunck_generator

import logging
from stream_unzip import stream_unzip


_logger = logging.getLogger(__name__)


def _psql_from_chunks(dbname, chunks):
    pg_args = [
        "psql",
        "--quiet",
        f"--dbname={dbname}",
    ]
    pg_env = odoo.tools.misc.exec_pg_environ()
    _logger.debug(f"Running {' '.join(pg_args)}")
    try:
        psql_popen = subprocess.Popen(
            pg_args,
            env=pg_env,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            for chunk in chunks:
                if psql_popen.poll() is not None:
                    # psql died early
                    break
                psql_popen.stdin.write(chunk)
            psql_popen.stdin.close()
            psql_popen.wait()
        except BrokenPipeError:
            _logger.error("Pipe broken: psql closed the connection early.")

        if psql_popen.returncode != 0:
            _logger.error("Postgres Restore Failed")
            raise click.ClickException(
                f"psql failed with exit code {psql_popen.returncode}"
            )

    except Exception as e:
        _logger.exception("Unexpected error during restore")
        raise click.ClickException("Couldn't restore database") from e


def _check_path(full_path):
    if os.path.exists(full_path):
        return
    parent_dir = os.path.dirname(full_path)
    _check_path(parent_dir)
    _logger.debug('created folder "%s"', full_path)
    os.mkdir(full_path)


def _restore_file_from_chunks(full_path, chunks):
    try:
        _logger.debug('restoring file "%s"', full_path)
        _check_path(os.path.dirname(full_path))
        with open(full_path, "wb") as fileh:
            for chunk in chunks:
                fileh.write(chunk)
    except IOError:
        _logger.exception("Error writing to file %s", full_path)
        # make sure to finish all chunks
        for chunk in chunks:
            pass


def _restore_from_zipchunks(dbname, zipped_chunks, copy=True, jobs=1, neutralize=False):
    filestore_folder = odoo.tools.config.filestore(dbname)

    for f_name, f_size, file_chunks in stream_unzip(zipped_chunks):
        file_name = f_name.decode("utf-8")
        if file_name == "dump.sql":
            _psql_from_chunks(dbname, file_chunks)
        elif file_name.startswith(f"{FILESTORE_DIRNAME}/"):
            full_path = os.path.join(
                filestore_folder, file_name.replace(f"{FILESTORE_DIRNAME}/", "")
            )
            _restore_file_from_chunks(full_path, file_chunks)
        elif file_name == "manifest.json":
            full_path = os.path.join(filestore_folder, "manifest.json")
            _restore_file_from_chunks(full_path, file_chunks)
        else:
            _logger.warning('Unexpected file "%s" in zip file: ignoring', file_name)
            # unzipped_chunks must be iterated to completion or
            # UnfinishedIterationError will be raised
            for chunk in file_chunks:
                pass


def _restore_from_url(dbname, source, copy, neutralize):
    chunks_from_url = url_chunck_generator(url=source)
    odoo.service.db._create_empty_database(dbname)
    _restore_from_zipchunks(
        dbname=dbname,
        zipped_chunks=chunks_from_url,
        copy=copy,
        neutralize=neutralize,
    )
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


def _restore_from_file(dbname, backup, copy=True, neutralize=False):
    with db_management_enabled():
        extra_kwargs = {}
        if odoo.release.version_info >= (16, 0):
            extra_kwargs["neutralize_database"] = neutralize
        odoo.service.db.restore_db(dbname, backup, copy, **extra_kwargs)
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
    if source.startswith("http"):
        _logger.debug('Restoring DB from "%s"', source)
        _restore_from_url(dbname, source, copy, neutralize)
    elif os.path.isfile(source):
        _restore_from_file(dbname, source, copy, neutralize)
    else:
        _restore_from_folder(dbname, source, copy, jobs, neutralize)
