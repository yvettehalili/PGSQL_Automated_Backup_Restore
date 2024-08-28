#!/usr/bin/python3.5

import os
import subprocess
import datetime
import configparser
import io
import time
import logging
from google.cloud import storage

# Constants and paths
BUCKET = "ti-sql-02"
GCS_PATH = "Backups/Current/POSTGRESQL"
SSL_PATH = "/ssl-certs/"
KEY_FILE = "/root/jsonfiles/ti-ca-infrastructure-d1696a20da16.json"

# Define the path for the database credentials and load them
CREDENTIALS_PATH = "/backup/configs/db_credentials.conf"
config = configparser.ConfigParser()
config.read(CREDENTIALS_PATH)
DB_USR = config['credentials']['DB_USR']
DB_PWD = config['credentials']['DB_PWD']

# Set environment variable for PostgreSQL password
os.environ["PGPASSWORD"] = DB_PWD

# Log file path
LOG_FILE_BASE_PATH = "/backup/logs/PGSQL_backup_activity"
CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d")
LOG_FILE_PATH = "{}_{}.log".format(LOG_FILE_BASE_PATH, CURRENT_DATE)

# Specific database roles
DB_ROLES = {
    "db_datti": "GenBackupUser",
    "db_gtt_historic_data": "GenBackupUser"
}

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

def run_command_capture(command, env=None):
    """Run a command and capture its output and error messages."""
    try:
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT, env=env or os.environ)
        return True, output.decode()
    except subprocess.CalledProcessError as e:
        return False, e.output.decode()

# Stream database to GCS
def stream_database_to_gcs(dump_command, gcs_path, db):
    start_time = time.time()

    try:
        logging.info("Starting dump process: {}".format(" ".join(dump_command)))

        # Start the dump process
        dump_proc = subprocess.Popen(dump_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Initialize Google Cloud Storage client
        client = storage.Client.from_service_account_json(KEY_FILE)
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(gcs_path)

        logging.info("Starting GCS upload process")
        with io.BytesIO() as memfile:
            for chunk in iter(lambda: dump_proc.stdout.read(4096), b''):
                memfile.write(chunk)

            memfile.seek(0)
            blob.upload_from_file(memfile, content_type='application/octet-stream')

        elapsed_time = time.time() - start_time
        logging.info("Dumped and streamed database {} to GCS successfully in {:.2f} seconds.".format(db, elapsed_time))

    except Exception as e:
        logging.error("Unexpected error streaming database {} to GCS: {}".format(db, e))

def main():
    # Initialize the configuration parser and load the server configurations
    server_config = configparser.ConfigParser()
    server_config.read('/backup/configs/PGSQL_servers_list.conf')

    log_to_file("================================== {} =============================================".format(CURRENT_DATE))

    # Read server configurations into a list of tuples
    servers = []
    for section in server_config.sections():
        SERVER = section
        HOST = server_config.get(section, 'host')
        SSL = server_config.get(section, 'ssl')
        servers.append((SERVER, HOST, SSL))

    for server in servers:
        SERVER, HOST, SSL = server
        DB_HOST = HOST

        log_to_file("DUMPING SERVER: {}".format(SERVER))

        if SSL == 'n':
            dbs_command = ("psql -h {} -U {} -d postgres -t -c 'SELECT datname FROM pg_database WHERE datistemplate = false;'"
                            ).format(DB_HOST, DB_USR)
        else:
            dbs_command = (
                "psql \"sslmode=verify-ca sslrootcert={}{}{} sslcert={}{}{} sslkey={}{}{} hostaddr={} user={} dbname=postgres\" "
                "-t -c 'SELECT datname FROM pg_database WHERE datistemplate = false;'"
            ).format(SSL_PATH, SERVER, "/server-ca.pem", SSL_PATH, SERVER, "/client-cert.pem",
                    SSL_PATH, SERVER, "/client-key.pem", DB_HOST, DB_USR)

        try:
            dbs_output = subprocess.check_output(dbs_command, shell=True, env=os.environ).decode().splitlines()
            dbs_output = [db.strip() for db in dbs_output if db.strip()]
        except subprocess.CalledProcessError as e:
            log_to_file("Failed to fetch databases from server {}: {}".format(SERVER, e))
            continue

        for DB in dbs_output:
            if DB not in ["template0", "template1", "restore", "postgres", "cloudsqladmin"]:
                log_to_file("Dumping DB {}".format(DB))

                # Determine the role for the specific database
                role = DB_ROLES.get(DB, "postgres")

                # Construct the pg_dump command based on SSL status
                if SSL == 'y':
                    pg_dump_command = [
                        "pg_dump",
                        "sslmode=verify-ca user={} hostaddr={} sslrootcert={} sslcert={} sslkey={} dbname={}".format(
                            DB_USR,
                            DB_HOST,
                            os.path.join(SSL_PATH, SERVER, "server-ca.pem"),
                            os.path.join(SSL_PATH, SERVER, "client-cert.pem"),
                            os.path.join(SSL_PATH, SERVER, "client-key.pem"),
                            DB
                        ),
                        "--role=postgres",
                        "--no-owner",
                        "--no-acl",
                        "-Fc"
                    ]
                else:
                    pg_dump_command = [
                        "pg_dump",
                        "postgresql://{}@{}:5432/{}".format(DB_USR, DB_HOST, DB),
                        "--role=postgres",
                        "--no-owner",
                        "--no-acl",
                        "-Fc"
                    ]

                gcs_path = "{}/{}/{}_{}.dump".format(GCS_PATH, SERVER, CURRENT_DATE, DB)
                if stream_database_to_gcs(pg_dump_command, gcs_path, DB):
                    log_to_file("Successfully backed up and streamed {} from server {} to GCS".format(DB, SERVER))
                else:
                    log_to_file("Failed to backup and stream {} from server {}".format(DB, SERVER))

    log_to_file("============================================================================================")

if __name__ == "__main__":
    main()
