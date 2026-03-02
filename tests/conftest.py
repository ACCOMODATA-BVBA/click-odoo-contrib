# tests/conftest.py
# Copyright 2018 ACSONE SA/NV (<http://acsone.eu>)
# License LGPL-3.0 or later (http://www.gnu.org/licenses/lgpl.html).

import os
import subprocess
import textwrap
import configparser

import pytest
from click_odoo import odoo

# This hack is necessary because the way CliRunner patches
# stdout is not compatible with the Odoo logging initialization
# mechanism. Logging is therefore tested with subprocesses.
odoo.netsvc.init_logger = lambda: None


def _has_docker_db():
    """Check of we in een Docker omgeving draaien met credentials"""
    config_path = os.getenv("ODOO_RC", "/etc/odoo/odoo.conf")
    if os.path.exists(config_path):
        config = configparser.ConfigParser()
        config.read(config_path)
        if config.has_option("options", "db_password"):
            return True

    if os.getenv("DB_PASSWORD"):
        return True

    return False


def _get_db_config():
    config_path = os.getenv("ODOO_RC", "/etc/odoo/odoo.conf")

    if os.path.exists(config_path):
        config = configparser.ConfigParser()
        config.read(config_path)
        return {
            "host": config.get("options", "db_host", fallback="localhost"),
            "port": config.get("options", "db_port", fallback="5432"),
            "user": config.get("options", "db_user", fallback="odoo"),
            "password": config.get("options", "db_password", fallback=""),
        }

    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "user": os.getenv("DB_USER", "odoo"),
        "password": os.getenv("DB_PASSWORD", ""),
    }


def _get_odoo_command():
    """
    Get the correct Odoo command for the environment.
    Returns either a string (for normal install) or a list (for Docker).
    """
    # Check if 'odoo' executable exists
    try:
        subprocess.run(["which", "odoo"], check=True, capture_output=True)
        return "odoo"
    except subprocess.CalledProcessError:
        pass

    # Check if 'odoo-bin' executable exists
    try:
        subprocess.run(["which", "odoo-bin"], check=True, capture_output=True)
        return "odoo-bin"
    except subprocess.CalledProcessError:
        pass

    # Fallback to python3 -m odoo (Docker scenario)
    return ["python3", "-m", "odoo"]


def _init_odoo_db(dbname, test_addons_dir=None):
    """
    Backwards compatible database initialisatie.
    Werkt zowel met lokale PostgreSQL als Docker setup.
    """
    if _has_docker_db():
        _init_odoo_db_with_credentials(dbname, test_addons_dir)
    else:
        _init_odoo_db_local(dbname, test_addons_dir)


def _init_odoo_db_local(dbname, test_addons_dir=None):
    """
    ORIGINELE MANIER - voor lokale development zonder Docker
    """
    subprocess.check_call(["createdb", dbname])

    odoo_cmd = _get_odoo_command()
    if isinstance(odoo_cmd, list):
        cmd = odoo_cmd.copy()
    else:
        cmd = [odoo_cmd]
    config_path = os.getenv("ODOO_RC", "/etc/odoo/odoo.conf")
    cmd.extend(["--config", config_path])
    cmd.extend(["-d", dbname, "-i", "base", "--stop-after-init"])

    if test_addons_dir:
        addons_path = [
            os.path.join(odoo.__path__[0], "addons"),
            os.path.join(odoo.__path__[0], "..", "addons"),
            test_addons_dir,
        ]
        cmd.extend(["--addons-path", ",".join(addons_path)])

    print(cmd)
    subprocess.check_call(cmd)


def _init_odoo_db_with_credentials(dbname, test_addons_dir=None):
    """
    NIEUWE MANIER - voor Docker setup met credentials
    """
    db_config = _get_db_config()

    env = os.environ.copy()
    env["PGPASSWORD"] = db_config["password"]

    # DROP database eerst als die bestaat
    print(f"Dropping database {dbname} if exists...")
    subprocess.call(
        [
            "dropdb",
            "-h",
            db_config["host"],
            "-p",
            str(db_config["port"]),
            "-U",
            db_config["user"],
            "--if-exists",
            dbname,
        ],
        env=env,
        stderr=subprocess.DEVNULL,
    )

    print(f"Creating database {dbname}...")
    subprocess.check_call(
        [
            "createdb",
            "-h",
            db_config["host"],
            "-p",
            str(db_config["port"]),
            "-U",
            db_config["user"],
            dbname,
        ],
        env=env,
    )

    config_path = os.getenv("ODOO_RC", "/etc/odoo/odoo.conf")

    cmd = ["python3", "-m", "odoo", "--config", config_path]
    cmd.extend(["-d", dbname, "-i", "base", "--stop-after-init"])

    if test_addons_dir:
        addons_path = [
            os.path.join(odoo.__path__[0], "addons"),
            os.path.join(odoo.__path__[0], "..", "addons"),
            test_addons_dir,
        ]
        cmd.extend(["--addons-path", ",".join(addons_path)])

    print(f"Running: {' '.join(cmd)}")
    subprocess.check_call(cmd, env=env)


def _drop_db(dbname):
    """
    Backwards compatible database cleanup
    """
    if _has_docker_db():
        _drop_db_with_credentials(dbname)
    else:
        _drop_db_local(dbname)


def _drop_db_local(dbname):
    """ORIGINELE MANIER"""
    subprocess.check_call(["dropdb", dbname])


def _drop_db_with_credentials(dbname):
    db_config = _get_db_config()

    env = os.environ.copy()
    env["PGPASSWORD"] = db_config["password"]

    subprocess.call(
        [
            "dropdb",
            "-h",
            db_config["host"],
            "-p",
            str(db_config["port"]),
            "-U",
            db_config["user"],
            "--if-exists",
            dbname,
        ],
        env=env,
        stderr=subprocess.DEVNULL,
    )


@pytest.fixture(scope="module")
def odoodb(request):
    """
    Backwards compatible odoodb fixture
    Werkt automatisch met beide setups!
    """
    dbname = f"click-odoo-contrib-test-{odoo.release.version_info[0]}"
    test_addons_dir = getattr(request.module, "test_addons_dir", "")

    _init_odoo_db(dbname, test_addons_dir)

    try:
        yield dbname
    finally:
        _drop_db(dbname)


@pytest.fixture(scope="function")
def odoocfg(request, tmpdir):
    addons_path = [
        os.path.join(odoo.__path__[0], "addons"),
        os.path.join(odoo.__path__[0], "..", "addons"),
    ]
    test_addons_dir = getattr(request.module, "test_addons_dir", "")
    if test_addons_dir:
        addons_path.append(test_addons_dir)
    odoo_cfg = tmpdir / "odoo.cfg"
    odoo_cfg.write(
        textwrap.dedent(
            f"""\
            [options]
            addons_path = {",".join(addons_path)}
            """
        )
    )
    yield odoo_cfg
