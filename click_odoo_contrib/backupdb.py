#!/usr/bin/env python
# Copyright 2018 ACSONE SA/NV (<http://acsone.eu>)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html).

import json
import os
import shutil
import subprocess

import click
import click_odoo
from click_odoo import odoo

from ._backup import backup
from ._dbutils import db_exists, db_management_enabled, pg_connect
from ._s3 import S3Client

MANIFEST_FILENAME = "manifest.json"
DBDUMP_FILENAME = "db.dump"
FILESTORE_DIRNAME = "filestore"
FILESTORE_FILE_REGEX = "^[a-f0-9]{2}/[a-f0-9]{40}"


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
    if backup.format == "stream-zip":
        backup.add_dump_command(cmd, env, filename)
    else:
        stdout = subprocess.Popen(
            cmd, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE
        ).stdout
        backup.write(stdout, filename)


def _get_filestore_file_list(cr, dbname, minimal=False):
    filestore_path = odoo.tools.config.filestore(dbname)
    if minimal:
        qry = (
            "SELECT DISTINCT store_fname "
            "FROM ir_attachment "
            "WHERE create_uid IN (1, 2) "
            "   AND store_fname IS NOT NULL "
            f"   AND store_fname ~ '{FILESTORE_FILE_REGEX}'"
            "   AND ("
            "       res_model IN ("
            "           'ir.ui.view',"
            "           'ir.ui.menu',"
            "           'res.company',"
            "           'res.lang' )"
            "          OR res_model IS NULL "
            "          OR name ILIKE '%assets%'"
            "   )"
        )
    else:
        qry = (
            "SELECT DISTINCT store_fname "
            "FROM ir_attachment "
            f"WHERE store_fname ~ '{FILESTORE_FILE_REGEX}'"
        )
    with pg_connect(dbname) as cr:
        cr.execute(qry)
        result = [os.path.join(filestore_path, item[0]) for item in cr.fetchall()]
    return result


def _create_manifest(cr, dbname, backup):
    manifest = odoo.service.db.dump_db_manifest(cr)
    backup.add_data(json.dumps(manifest, indent=4).encode("utf-8"), MANIFEST_FILENAME)


def _backup_filestore(dbname, backup):
    filestore_source = odoo.tools.config.filestore(dbname)
    if os.path.isdir(filestore_source):
        backup.addtree(filestore_source, FILESTORE_DIRNAME)


def _backup_filestore_adv(cr, dbname, backup, minimal):
    filestore_source = odoo.tools.config.filestore(dbname)
    len_prefix = len(filestore_source) + 1
    for fname in _get_filestore_file_list(cr, dbname, minimal):
        path = os.path.normpath(fname)
        if os.path.isfile(path):
            _arcname = os.path.join(FILESTORE_DIRNAME, path[len_prefix:])
            backup.addfile(path, _arcname)


def _get_s3_client(
    aws_access_key_id=None,
    aws_secret_access_key=None,
    aws_endpoint_url=None,
    aws_region=None,
    aws_bucketname=None,
):
    missing = []
    if aws_access_key_id is None:
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        if aws_access_key_id is None:
            missing.append("aws_access_key_id")
    if aws_secret_access_key is None:
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if aws_secret_access_key is None:
            missing.append("aws_secret_access_key")
    if aws_endpoint_url is None:
        aws_endpoint_url = os.getenv("AWS_ENDPOINT_URL")
        if aws_endpoint_url is None:
            missing.append("aws_endpoint_url")
    if aws_region is None:
        aws_region = os.getenv("AWS_REGION")
        if aws_region is None:
            missing.append("aws_region")
    if aws_bucketname is None:
        aws_bucketname = os.getenv("AWS_BUCKETNAME")
        if aws_bucketname is None:
            missing.append("aws_bucketname")

    if missing:
        raise Exception("Missing s3 credentials: ", ",".join(missing))

    return S3Client(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=aws_endpoint_url,
        region_name=aws_region,
        bucket=aws_bucketname,
    )


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
    type=click.Choice(["zip", "dump", "folder", "stream-zip"]),
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
@click.option(
    "--aws_access_key_id",
    default=None,
    show_default=False,
    help="Specify s3 aws_access_key_id or define environment value AWS_ACCESS_KEY_ID",
)
@click.option(
    "--aws_secret_access_key",
    default=None,
    show_default=False,
    help="Specify s3 aws_secret_access_key or define environment value AWS_SECRET_ACCESS_KEY",
)
@click.option(
    "--aws_endpoint_url",
    default=None,
    show_default=False,
    help="Specify s3 aws_endpoint_url or define environment value AWS_ENDPOINT_URL",
)
@click.option(
    "--aws_region",
    default=None,
    show_default=False,
    help="Specify s3 aws_region or define environment value AWS_REGION",
)
@click.option(
    "--aws_bucketname",
    default=None,
    show_default=False,
    help="Specify s3 aws_bucketname or define environment value AWS_BUCKETNAME",
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
    aws_access_key_id,
    aws_secret_access_key,
    aws_endpoint_url,
    aws_region,
    aws_bucketname,
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
    if format == "stream-zip":
        s3_client = _get_s3_client(
            aws_access_key_id,
            aws_secret_access_key,
            aws_endpoint_url,
            aws_region,
            aws_bucketname,
        )
        s3_client.test_access_upload(dest)

    if format == "dump":
        filestore = False
    db = odoo.sql_db.db_connect(dbname)
    try:
        with backup(
            format, dest, "w"
        ) as _backup, db.cursor() as cr, db_management_enabled():
            if format != "dump":
                _create_manifest(cr, dbname, _backup)
            if filestore == "minimal":
                _backup_filestore_adv(cr, dbname, _backup, minimal=True)
            elif filestore == "full":
                _backup_filestore_adv(cr, dbname, _backup, minimal=False)

            _dump_db(dbname, _backup)
        if format == "stream-zip":
            s3_client.upload_fileh(dest, _backup.get_file_object())
    finally:
        odoo.sql_db.close_db(dbname)


if __name__ == "__main__":  # pragma: no cover
    main()
