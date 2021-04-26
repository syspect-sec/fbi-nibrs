# NIBRSParser.py
# NIBRS Bulk Data Parser - Main Parser
# Description: Processes all NIBRS files.
# Author: Joseph Lee
# Email: joseph@ripplesoftware.ca
# Website: www.ripplesoftware.ca
# Github: www.github.com/rippledj/fbi-nibrs

# Import modules
import os
import sys
import traceback
import requests
import urllib
import NIBRSLogger
import NIBRSSanitizer
import SQLProcessor
import time
import ssl
import shutil
import zipfile


# Get list of state codes from file
def get_state_codes_from_file(args):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
    logger.info("-- Getting list state codes from file...")
    print("-- Getting list state codes from file...")

    state_codes = []
    with open(args['state_codes'], "r") as infile:
        for line in infile:
            line = line.split(",")
            state_codes.append(line[0].replace('"', ''))
    #print(state_codes)
    return state_codes

# Get a list of all .csv files in a directory and return list
def get_csv_files_from_directory(item, args):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
    logger.info("-- Looking for .csv files in: " + item['extract_directory'] + "...")
    print("-- Looking for .csv files in: " + item['extract_directory'] + "...")

    csv_files = []
    # Get list of files in extracted directory
    all_files = os.listdir(item['extract_directory'])
    for found in all_files:
        if found.split(".")[-1] == "csv" or found.split(".")[-1] == "CSV":
            csv_files.append(found)

    # Check for directory that uses state code
    if os.path.isdir(item['extract_directory'] + item['state_code']):
        logger.info("-- Subdirectory " + item['state_code'] + " found while looking for .csv files...")
        print("-- Subdirectory " + item['state_code'] + " found while looking for .csv files...")
        all_files = os.listdir(item['extract_directory'] + item['state_code'])
        for found in all_files:
            if found.split(".")[-1] == "csv" or found.split(".")[-1] == "CSV":
                csv_files.append(item['state_code'] + "/"+ found)

    # Return list of .csv files
    return csv_files

# Get link list from log file
def get_link_list_from_log(args):
    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
    print("-- Checking for existing link log file...")
    logger.info("-- Checking for existing link log file...")

    # Get existing log records
    link_list = []
    with open(args['link_log_file'], "r") as infile:
        for line in infile:
            line = line.split(",")
            link_list.append({ "url" : line[0], "status" : line[1].strip()})
    print("-- " + str(len(link_list)) + " existing links log found in log file...")
    logger.info("-- " + str(len(link_list)) + " existing links log found in log file...")

    # Return the list of links
    return link_list

# Write new link list to log file
def write_link_list_to_log(link_list, args):
    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
    logger.info("-- Writing list of bulk data urls to log file...")
    print("-- Writing list of bulk data urls to log file...")

    with open(args['link_log_file'], "w") as outfile:
        for link in link_list:
            outfile.write(link['url'] + "," + link['status'] + "\n")

    logger.info("-- Finished writing list of bulk data urls to log file...")
    print("-- Finished writing list of bulk data urls to log file...")

# Mark the link url as processed in log file
def mark_link_as_processed(link, args):
    pass

# Get a list of all links
def get_link_list(args):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
    logger.info("-- Getting list of bulk data urls...")

    # Check if log of links exists
    if os.path.exists(args['link_log_file']):
        link_list = get_link_list_from_log(args)
        return link_list

    # If no existing link log found
    else:
        print("-- No existing link log file found...")
        logger.info("-- No existing link log file found...")
        link_list = []
        # Get list of state codes from file
        state_codes = get_state_codes_from_file(args)
        # Create log by enumerating all state codes and years to file files
        for state in state_codes:
            year = args['start_year']
            while year <= args['end_year']:
                # Iterate through each possible url ext
                for ext in args['url_ext']:
                    url = args['base_url'] + ext + "/" + str(year) + "/" + state + "-" + str(year) + ".zip"
                    print("-- Checking url: " + url)
                    r = requests.head(url)
                    print(r.status_code)
                    print(r.headers)
                    if r.status_code == 200:
                        print("** Link found: " + url)
                        # Append to link_list
                        link_list.append({ "url" : url, "status" : "Unprocessed"})
                    else:
                        print("-- Bad link: " + url)
                # Increment year
                year += 1
        # Write link list to log
        write_link_list_to_log(link_list, args)
        # Return the list of links
        return link_list


# Extract downloaded zip file
def extract_zip_file(item, args):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
    logger.info("[ Extracting " + item['zip_filename'] + " NIBRS .zip file ]")
    print("[ Extracting " + item['zip_filename'] + " NIBRS .zip file ]")

    # Check for directory to unzip contents to
    if not os.path.exists(item['extract_directory']):
        os.mkdir(item['extract_directory'])
    # Unzip the downloaded file
    try:
        with zipfile.ZipFile(item['dl_filename'], 'r') as zip_ref:
            zip_ref.extractall(item['extract_directory'])
        # Return success state
        return True
    except Exception as e:
        # Print and log general fail comment
        print("-- Exception during extraction of " + item['dl_filename'])
        logger.error("-- Exception during extraction of " + item['dl_filename'])
        traceback.print_exc()
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
        # Return failed state
        return False

# Process single link
def download_and_extract_single_link(link, args):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")

    # Download the file from Amazon AWS
    if os.path.exists(link['dl_filename']):
        logger.info("-- Previously downloaded bulk data found: " + link['dl_filename'] + "...")
        print("-- Previously downloaded bulk data found: " + link['dl_filename'] + "...")

    # If the file was not found donwloaded already
    else:
        # Set the context for SSL (not checking!)
        context = ssl.SSLContext()
        with urllib.request.urlopen(link['url'], context=context) as response, open(link['dl_filename'], 'wb') as out_file:
            shutil.copyfileobj(response, out_file)

    # Extract the downloaded zip file
    extract_success = extract_zip_file(link, args)
    if extract_success: return True
    else:
        os.remove(link['dl_filename'])
        return False

# Process item into database
def insert_item_into_database(item, args):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
    logger.info("-- Starting to store " + item['base_filename'] + " to database...")
    print("-- Starting to store " + item['base_filename'] + " to database...")

    # Load the csv files into database
    for csv_file in item['csv_files']:
        item['filename'] =  csv_file
        item['full_filepath'] = item['extract_directory'] + csv_file
        args['db_conn'].load_csv_bulk_data(item, args)

    logger.info("-- Finished storing " + item['base_filename'] + " to database...")
    print("-- Finished storing " + item['base_filename'] + " to database...")

    return True

# Process all links
def process_all_links(args):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
    logger.info("-- Starting to process list of bulk data urls...")
    print("-- Starting to process list of bulk data urls...")

    for link in args['link_list']:
        link_success = False
        # Get a filename and destination extracted data directory for link
        link['zip_filename'] = link['url'].split("/")[-1]
        link['dl_filename'] = args['dl_dir'] + link['zip_filename']
        link['base_filename'] = link['zip_filename'].split(".")[0]
        link['extract_directory'] = args['dl_dir'] + link['zip_filename'].split(".")[0] + "/"

        # Get the state and year of each file
        link['state_code'] = link['base_filename'].split("-")[0]
        link['year'] = int(link['base_filename'].split("-")[1])

        logger.info("-- Checking status of link: " + link['zip_filename'] + " - " + link['status'] + "...")
        print("-- Checking status of link: " + link['zip_filename'] + " - " + link['status'] + "...")

        # Check if file donwloaded already
        if link['status'] == "Unprocessed":
            logger.info("-- Starting to process link: " + link['url'] + "...")
            print("-- Starting to process link: " + link['url'] + "...")
            while not link_success:
                extract_success = download_and_extract_single_link(link, args)
                if extract_success:
                    insert_success = False
                    # Get list of .csv files in the unzipped directory
                    # Each directory also contains .SQL files
                    link['csv_files'] = get_csv_files_from_directory(link, args)
                    insert_success = NIBRSSanitizer.sanitize_csv_files(link, args)
                    #insert_success = insert_item_into_database(link, args)
                    if insert_success:
                        #mark_link_as_processed(link, args)
                        link_success = True

                else:
                    logger.info("-- Failed to download and extract link: " + link['url'] + "...")
                    print("-- Failed to download and extract link: " + link['url'] + "...")
        else:
            logger.info("-- Skipping previously procesed link: " + link['url'] + "...")
            print("-- Skipping previously procesed link: " + link['url'] + "...")

#
# Main Function
#
if __name__ == "__main__":

    # Declare vars
    cwd = os.getcwd() + "/"
    app_log_file = cwd + "LOG/NIBRS_app.log"
    link_log_file = cwd + "LOG/NIBRS.log"
    # Log levels
    log_level = 3 # Log levels 1 = error, 2 = warning, 3 = info
    stdout_level = 1 # Stdout levels 1 = verbose, 0 = non-verbose
    # Declare variables
    start_time = time.time()

    # Default number of threads to use if not specified.
    # 5 threads is good on 4 core processor.
    # General rule of 1 thread per core, plus one seems to work well.
    default_threads = 5

    # Declare file locations
    dl_dir = cwd + "TMP/downloads/"
    state_codes = cwd + "RES/abbr-name.csv"
    main_table_names_file = cwd + "RES/main_table_names.txt"
    code_table_names_file = cwd + "RES/code_table_names.txt"
    adjustment_req_filename = cwd + "RES/adjustment_req_filename.txt"

    # Database args
    database_args = {
        "database_type" : "postgresql", # choose 'mysql' or 'postgresql'
        #"database_type" : "mysql", # choose 'mysql' or 'postgresql'
        "host" : "127.0.0.1",
        "port" : 5432, # PostgreSQL port
        #"port" : 3306, # MySQL port
        "user" : "nibrs",
        "passwd" : "Ld58KimTi06v2PnlXTFuLG4", # PostgreSQL password
        #"passwd" : "R5wM9N5qCEU3an#&rku8mxrVBuF@ur", # MySQL password
        "db" : "nibrs",
        "charset" : "utf8"
    }


    # Declare args
    args = {
        "cwd" : cwd,
        "app_log_file" : app_log_file,
        "link_log_file" : link_log_file,
        "log_level" : log_level,
        "stdout_level" : stdout_level,
        "default_threads" : default_threads,
        "dl_dir" : dl_dir,
        "state_codes" : state_codes,
        "main_table_names_file" : main_table_names_file,
        "code_table_names_file" : code_table_names_file,
        "adjustment_req_filename" : adjustment_req_filename,
        "start_year" : 1991,
        "end_year" : 2020,
        "database_args" : database_args,
        # Example full URLs
        # https://s3-us-gov-west-1.amazonaws.com/cg-d4b776d0-d898-4153-90c8-8336f86bdfec/2019/MD-2019.zip
        # https://s3-us-gov-west-1.amazonaws.com/cg-d3f0433b-a53e-4934-8b94-c678aa2cbaf3/2007/AL-2007.zip
        "base_url" : "https://s3-us-gov-west-1.amazonaws.com/",
        "url_ext" : [
            "cg-d4b776d0-d898-4153-90c8-8336f86bdfec",
            "cg-d3f0433b-a53e-4934-8b94-c678aa2cbaf3",
        ]

    }

    # Setup logger
    NIBRSLogger.setup_logger(args['log_level'], app_log_file)
    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")

    # Create a database connection
    db_conn = SQLProcessor.SQLProcess(database_args)
    db_conn.connect()
    args['db_conn'] = db_conn

    # Build list of links
    args['link_list'] = get_link_list(args)
    # Download and process each link into database
    process_all_links(args)
