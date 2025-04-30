import os
import logging
import configparser
import subprocess
import io
import time
from google.cloud import storage
import datetime


# Backup and log path
BUCKET = "ti-dba-prod-01"
GCS_PATH = "Backups/Current/MYSQL"
SSL_PATH = "/ssl-certs/"
SERVERS_LIST = "/backup/configs/tiaiproduction-MYSQL_database_list.conf"  # Updated file name
KEY_FILE = "/root/jsonfiles/ti-dba-prod-01.json"

# Define the path for the database credentials
CREDENTIALS_PATH = "/backup/configs/db_credentials.conf"

# Logging Configuration
log_path = "/backup/logs/"
os.makedirs(log_path, exist_ok=True)
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
log_filename = os.path.join(log_path, "tiai_MYSQL_backup_activity_{}.log".format(current_date))
logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s")


# Load Database credentials
config = configparser.ConfigParser()
config.read(CREDENTIALS_PATH)
DB_USR = config['credentials']['DB_USR']
DB_PWD = config['credentials']['DB_PWD']

def sanitize_command(command):
    """Sanitize the command by replacing sensitive information."""
    sanitized_command = [
        arg.replace(DB_USR, "*****").replace(DB_PWD, "*****") if isinstance(arg, str) else arg
        for arg in command
    ]
    return sanitized_command

def load_server_list(file_path):
    """Load the server list from a given file."""
    config = configparser.ConfigParser()
    try:
        config.read(file_path)
        return config.sections(), config
    except Exception as e:
        logging.error("Failed to load server list: {}".format(e))
        return [], None

def stream_database_to_gcs(dump_command, gcs_path, db):
    start_time = time.time()
    try:
        sanitized_command = sanitize_command(dump_command)
        logging.info("Starting dump process: {}".format(" ".join(sanitized_command)))

        # Start the dump process
        dump_proc = subprocess.Popen(dump_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info("Starting gzip process")

        # Start the gzip process
        gzip_proc = subprocess.Popen(["gzip"], stdin=dump_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Initialize Google Cloud Storage client
        client = storage.Client.from_service_account_json(KEY_FILE)
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(gcs_path)
        logging.info("Starting GCS upload process")

        # Stream the compressed data to GCS
        with io.BytesIO() as memfile:
            for chunk in iter(lambda: gzip_proc.stdout.read(4096), b''):
                memfile.write(chunk)
            memfile.seek(0)
            blob.upload_from_file(memfile, content_type='application/gzip')

        elapsed_time = time.time() - start_time
        logging.info("Dumped and streamed database {} to GCS successfully in {:.2f} seconds.".format(db, elapsed_time))
    except Exception as e:
        # Fixed formatting issue here
        logging.error("Unexpected error streaming database {} to GCS: {}".format(db, str(e)))

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
            databases = config[section].get('databases', "").split(",")  # Get specific databases to backup
            servers.append((section, host, ssl, [db.strip() for db in databases]))
        except KeyError as e:
            logging.error("Missing configuration for server '{}': {}".format(section, str(e)))
    for server in servers:
        SERVER, HOST, SSL, DB_LIST = server
        use_ssl = SSL.lower() == "y"
        logging.info("DUMPING SERVER: {}".format(SERVER))
        try:
            for db in DB_LIST:
                logging.info("Backing up database: {}".format(db))
                gcs_path = os.path.join(GCS_PATH, SERVER, "{}_{}.sql.gz".format(current_date, db))
                dump_command = [
                    "mysqldump", "-u{}".format(DB_USR), "-p{}".format(DB_PWD), "-h", HOST, db,
                    "--set-gtid-purged=OFF", "--single-transaction", "--quick",
                    "--triggers", "--events", "--routines"
                ]
                if use_ssl:
                    dump_command += [
                        "--ssl-ca={}".format(os.path.join(SSL_PATH, SERVER, "server-ca.pem")),
                        "--ssl-cert={}".format(os.path.join(SSL_PATH, SERVER, "client-cert.pem")),
                        "--ssl-key={}".format(os.path.join(SSL_PATH, SERVER, "client-key.pem")),
                        "--ssl-mode=VERIFY_CA"
                    ]
                sanitized_command = sanitize_command(dump_command)
                logging.info("Dump command: {}".format(" ".join(sanitized_command)))
                stream_database_to_gcs(dump_command, gcs_path, db)
        except Exception as e:
            # Fixed formatting issue here
            logging.error("Error processing server {}: {}".format(SERVER, str(e)))

    logging.info("==== Backup Process Completed ====")

if __name__ == "__main__":
    main()
