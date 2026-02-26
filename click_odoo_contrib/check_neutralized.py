#!/usr/bin/env python
# Copyright 2026 Accomodata (https://www.accomodata.be/)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html).

import click
import click_odoo
from click_odoo import odoo
from click_odoo_contrib._dbutils import db_exists, pg_connect
import sys


@click.command()
@click_odoo.env_options(
    default_log_level="warn", with_database=False, with_rollback=False
)
@click.argument("dbname", nargs=1)
def main(env, dbname):
    """List Odoo databases."""
    if not db_exists(dbname):
        click.echo(
            click.style(
                f"Database {dbname} not found",
                fg="red",
            )
        )
        return False
    with pg_connect(dbname) as cr:
        cr.execute(
            """
            SELECT count(*) FROM ir_config_parameter
            WHERE key='database.is_neutralized' and value='true'
            """
        )
        result = cr.fetchone()
        is_neutralized = result and result[0] == 1
    odoo.sql_db.close_db(dbname)
    if is_neutralized:
        click.echo(
            click.style(
                f"Database {dbname} is neutralized",
                fg="green",
            )
        )
    else:
        click.echo(
            click.style(
                f"Database {dbname} is not neutralized",
                fg="red",
            )
        )
    return is_neutralized


if __name__ == "__main__":
    result = main()
    if not result:
        sys.exit(1)
