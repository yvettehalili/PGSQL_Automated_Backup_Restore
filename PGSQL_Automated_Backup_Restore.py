#!/usr/bin/python3.5

import os
import subprocess
import datetime

# Constants and paths
TMP_PATH = "/backup/"
SSL_PATH = "/ssl-certs/"

# Database credentials (to be updated as needed)
DB_USR = "GenBackupUser"
DB_PWD = "DBB@ckuPU53r*"

# Set environment variable for PostgreSQL password
os.environ["PGPASSWORD"] = DB_PWD

# Log file path
LOG_FILE_BASE_PATH = "/backup/logs/adhoc_PGSQL_backup_activity"
CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d")
LOG_FILE_PATH = "{}_{}.log".format(LOG_FILE_BASE_PATH, CURRENT_DATE)

def log_to_file(message):
    """Write messages to the log file with a timestamp."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE_PATH, "a") as log_file:
        log_file.write("{}: {}\n".format(timestamp, message))

def run_command(command, env=None):
    """Run a command and return its success."""
    try:
        subprocess.check_call(command, shell=True, env=env or os.environ)
        return True
    except subprocess.CalledProcessError as e:
        log_to_file("Command failed: {}\nError message: {}".format(command, e))
        return False

def backup_database(db_usr, db_pwd, db_host, db_name, use_ssl, dump_file_path, log_file_path):
    """Backup the specified database."""
    if use_ssl:
        pg_dump_command = (
            "nohup pg_dump --verbose --format=c "
            "\"sslmode=verify-ca user={} hostaddr={} sslrootcert={}{}{} sslcert={}{}{} sslkey={}{}{} dbname={}\" --role={} --no-owner --no-acl --no-comments > {} 2>> {} &"
        ).format(
            db_usr, db_host, SSL_PATH, "server", "/server-ca.pem",
            SSL_PATH, "server", "/client-cert.pem", SSL_PATH, "server", "/client-key.pem", 
            db_name, "postgres", dump_file_path, log_file_path
        )
    else:
        pg_dump_command = (
            "nohup pg_dump --verbose --format=c --no-owner --no-acl --no-comments --dbname=postgresql://{}@{}:5432/{} --role={} > {} 2>> {} &"
        ).format(db_usr, db_host, db_name, "postgres", dump_file_path, log_file_path)
    
    return run_command(pg_dump_command, env=os.environ)

def drop_database_if_exists(db_usr, db_pwd, db_host, db_name):
    """Drop the database on the target server if it exists."""
    drop_command = (
        "PGPASSWORD='{}' psql --host={} --port=5432 --username={} -c \"DROP DATABASE IF EXISTS {};\""
    ).format(db_pwd, db_host, db_usr, db_name)
    return run_command(drop_command, env=os.environ)

def restore_database(db_usr, db_pwd, db_host, dump_file_path, db_name, log_file_path):
    """Restore the specified database."""
    restore_command = (
        "nohup pg_restore --host={} --port=5432 --username={} --dbname={} --verbose {} 2>> {} &"
    ).format(db_host, db_usr, db_name, dump_file_path, log_file_path)
    return run_command(restore_command, env=os.environ)

def delete_backup_file(dump_file_path):
    """Delete the backup file."""
    try:
        os.remove(dump_file_path)
        log_to_file("Deleted backup file: {}".format(dump_file_path))
        return True
    except Exception as e:
        log_to_file("Failed to delete backup file: {}\nError: {}".format(dump_file_path, e))
        return False

def main():
    # Configuration for the backup server (source)
    backup_server = {
        'name': 'ti-postgresql-us-we-a-03',
        'host': '172.19.227.39',
        'ssl': 'n',
        'databases': ['db_datti']
    }                                                                                                                                            

    # Configuration for the restore server (target)
    restore_server = {
        'name': 'pgdump-test',
        'host': '34.42.166.61',
        'ssl': 'n',  # Assuming there is no SSL needed for restore server
        'databases': ['db_datti']
    }
    log_to_file("================================== {} =============================================".format(CURRENT_DATE))

    # Backup server details
    BACKUP_SERVER = backup_server['name']
    BACKUP_DB_HOST = backup_server['host']
    USE_SSL = backup_server['ssl'] == 'y'
    databases = backup_server['databases']

    # Restore server details
    RESTORE_SERVER = restore_server['name']
    RESTORE_DB_HOST = restore_server['host']
    restore_databases = restore_server['databases']

    log_to_file("DUMPING SERVER: {}".format(BACKUP_SERVER))

    for DB, TARGET_DB in zip(databases, restore_databases):
        log_to_file("Dumping DB {}".format(DB))

        # Construct dump file path
        dump_file_path = os.path.join(TMP_PATH, "{}_{}_{}.dump".format(CURRENT_DATE, BACKUP_SERVER, DB))

        # Backup database
        if backup_database(DB_USR, DB_PWD, BACKUP_DB_HOST, DB, USE_SSL, dump_file_path, LOG_FILE_PATH):
            log_to_file("Successfully backed up {} from server {}".format(DB, BACKUP_SERVER))
        else:
            log_to_file("Failed to backup {} from server {}".format(DB, BACKUP_SERVER))
            continue

        # Drop target database if it exists
        log_to_file("Dropping target database {}".format(TARGET_DB))
        if drop_database_if_exists(DB_USR, DB_PWD, RESTORE_DB_HOST, TARGET_DB):
            log_to_file("Successfully dropped database {}".format(TARGET_DB))
        else:
            log_to_file("Failed to drop database {}".format(TARGET_DB))

        # Restore database
        log_to_file("Restoring database {} to target server".format(TARGET_DB))
        if restore_database(DB_USR, DB_PWD, RESTORE_DB_HOST, dump_file_path, TARGET_DB, LOG_FILE_PATH):
            log_to_file("Successfully restored {} to target server {}".format(TARGET_DB, RESTORE_SERVER))
            # Delete backup file after successful restoration
            delete_backup_file(dump_file_path)
        else:
            log_to_file("Failed to restore {} to target server {}".format(TARGET_DB, RESTORE_SERVER))

    log_to_file("============================================================================================")

if __name__ == "__main__":
    main()
