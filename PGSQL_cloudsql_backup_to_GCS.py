#!/usr/bin/python3.5

import os
import time
import subprocess
import datetime
import logging
import configparser
import io
from google.cloud import storage

# Backup and log path
BUCKET = "ti-sql-02"
GCS_PATH = "Backups/Current/PGSQL"
SSL_PATH = "/ssl-certs/"
SERVERS_LIST = "/backup/configs/PGSQL_servers_list.conf"
KEY_FILE = "/root/jsonfiles/ti-ca-infrastructure-d1696a20da16.json"

# Define the path for the database credentials
CREDENTIALS_PATH = "/backup/configs/db_credentials.conf"

# Logging Configuration
log_path = "/backup/logs/"
os.makedirs(log_path, exist_ok=True)
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
log_filename = os.path.join(log_path, "PGSQL_backup_activity_{}.log".format(current_date))
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Load Database credentials
config = configparser.ConfigParser()
config.read(CREDENTIALS_PATH)
DB_USR = config['credentials']['DB_USR']
DB_PWD = config['credentials']['DB_PWD']

def load_server_list(file_path):
    """Load the server list from a given file."""
    config = configparser.ConfigParser()
    try:
        config.read(file_path)
        return config.sections(), config
    except Exception as e:
        logging.error("Failed to load server list: {}".format(e))
        return [], None

def get_database_list(host, use_ssl, server):
    """Retrieve the list of databases from the PostgreSQL server."""
    try:
        if not use_ssl:
            command = [
                "psql", "-U", DB_USR, "-h", host, "-lqt"
            ]
        else:
            command = [
                "psql", "-U", DB_USR, "-h", host, "--set=sslmode=verify-ca",
                "--set=sslrootcert=" + os.path.join(SSL_PATH, server, "server-ca.pem"),
                "--set=sslcert=" + os.path.join(SSL_PATH, server, "client-cert.pem"),
                "--set=sslkey=" + os.path.join(SSL_PATH, server, "client-key.pem"), "-lqt"
            ]

        result = subprocess.check_output(command, stderr=subprocess.STDOUT)
        db_list = result.decode("utf-8").strip().split('\n')
        valid_db_list = [
            db.split('|')[0].strip() for db in db_list if db.split('|')[0].strip() not in (
                "postgres", "template0", "template1"
            )
        ]

        return valid_db_list
    except subprocess.CalledProcessError as e:
        logging.error("Failed to get database list from {}: {} - Output: {}".format(
            host, e, e.output.decode()
        ))
        return []

# Stream database to GCS
def stream_database_to_gcs(dump_command, gcs_path, db):
    start_time = time.time()

    try:
        logging.info("Starting dump process: {}".format(" ".join(dump_command)))

        # Start the dump process
        dump_proc = subprocess.Popen(dump_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        logging.info("Starting gzip process")
        # Start the gzip process
        gzip_proc = subprocess.Popen(["gzip"], stdin=dump_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        dump_proc.stdout.close()  # Allow dump_proc to receive a SIGPIPE if gzip_proc exits

        # Initialize Google Cloud Storage client
        client = storage.Client.from_service_account_json(KEY_FILE)
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(gcs_path)

        logging.info("Starting GCS upload process")
        with io.BytesIO() as memfile:
            for chunk in iter(lambda: gzip_proc.stdout.read(4096), b''):
                memfile.write(chunk)

            memfile.seek(0)
            blob.upload_from_file(memfile, content_type='application/gzip')

        elapsed_time = time.time() - start_time
        logging.info("Dumped and streamed database {} to GCS successfully in {:.2f} seconds.".format(db, elapsed_time))

    except Exception as e:
        logging.error("Unexpected error streaming database {} to GCS: {}".format(db, e))

def main():
    """Main function to execute the backup process."""
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    sections, config = load_server_list(SERVERS_LIST)
    if not sections:
        logging.error("No servers to process. Exiting.")
        return

    logging.info("================================== {} =============================================".format(current_date))
    logging.info("==== Backup Process Started ====")
    servers = []
    for section in sections:
        try:
            host = config[section]['host']
            ssl = config[section].get('ssl', 'n')  # Provide default value 'n' if 'ssl' key is missing
            servers.append((section, host, ssl))
        except KeyError as e:
            logging.error("Missing configuration for server '{}': {}".format(section, e))

    for server in servers:
        SERVER, HOST, SSL = server
        use_ssl = SSL.lower() == "y"
        logging.info("DUMPING SERVER: {}".format(SERVER))

        try:
            db_list = get_database_list(HOST, use_ssl, SERVER)
            if not db_list:
                logging.warning("No databases found for server: {}".format(SERVER))
                continue

            for db in db_list:
                logging.info("Backing up database: {}".format(db))
                gcs_path = os.path.join(GCS_PATH, SERVER, "{}_{}.sql.gz".format(current_date, db))
                
                if use_ssl:
                    dump_command = [
                        "pg_dump", f"sslmode=verify-ca user={DB_USR} hostaddr={HOST} sslrootcert={SSL_PATH}/{SERVER}/server-ca.pem sslcert={SSL_PATH}/{SERVER}/client-cert.pem sslkey={SSL_PATH}/{SERVER}/client-key.pem dbname={db}",
                        "--role=postgres", "--no-owner", "--no-acl", "-Fc"
                    ]
                else:
                    dump_command = [
                        "pg_dump", f"postgresql://{DB_USR}:{DB_PWD}@{HOST}:5432/{db}",
                        "--role=postgres", "--no-owner", "--no-acl", "-Fc"
                    ]

                logging.info("Dump command: {}".format(" ".join(dump_command)))
                stream_database_to_gcs(dump_command, gcs_path, db)

        except Exception as e:
            logging.error("Error processing server {}: {}".format(SERVER, e))

    logging.info("==== Backup Process Completed ====")

if __name__ == "__main__":
    main()
