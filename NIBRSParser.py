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
import SQLProcessor
import time

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
    print(state_codes)
    return state_codes

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
            link_list.append({ "url" : line[0], "status" : line[1]})
    print("-- " + str(len(link_list)) + " existing links log found in log file...")
    logger.info("-- " + str(len(link_list)) + " existing links log found in log file...")
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
    logger.info("[ Extracting downloaded NIBRS .zip file ]")
    print("[ Extracting downloaded NIBRS .zip file ]")

    # Check for directory to unzip contents to
    if not os.path.exists(item['extract_directory']):
        os.mkdir(item['extract_directory'])
    with zipfile.ZipFile(item['dl_filename'], 'r') as zip_ref:
        zip_ref.extractall(item['extract_directory'])
    return True

# Process single link
def process_single_link(item, args):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")

    # Download the file from Amazon AWS
    if os.path.exists(item['dl_filename']):
        logger.info("-- Previously downloaded bulk data found: " + item['dl_filename'] + "...")
        print("-- Previously downloaded bulk data found: " + item['dl_filename'] + "...")

    # If the file was not found donwloaded already
    else:
        # Set the context for SSL (not checking!)
        context = ssl.SSLContext()
        with urllib.request.urlopen(item['url'], context=context) as response, open(item['dl_filename'], 'wb') as out_file:
            shutil.copyfileobj(response, out_file)

    # Extract the downloaded zip file
    extract_successs = extract_zip_file(item, args)
    if extract_success :return True
    else:
        os.remove(item['dl_filename'])
        return False

# Process all links
def process_all_links(args):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
    logger.info("-- Process list of bulk data urls...")

    for item in args['link_list']:
        success = False
        # Get a filename and destination extracted data directory for item
        item['zip_filename'] = item['url'].split("/")[-1]
        item['dl_filename'] = item['dl_dir'] + item['zip_filename']
        item['extract_directory'] = item['dl_dir'] + item['zip_filename'].split(".")[0]

        # Check if file donwloaded already
        if item['status'] == "Unprocessed":
            while not success:
                extract_success = process_single_link(item, args)
                if extract_successs:
                    process_item_into_database(item, args)
                    successs = True
        else:
            logger.info("-- Skipping previously procesed link " + + "...")

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

    # Database args
    database_args = {
        #"database_type" : "postgresql", # choose 'mysql' or 'postgresql'
        "database_type" : "mysql", # choose 'mysql' or 'postgresql'
        "host" : "127.0.0.1",
        #"port" : 5432, # PostgreSQL port
        "port" : 3306, # MySQL port
        "user" : "uspto",
        #"passwd" : "Ld58KimTi06v2PnlXTFuLG4", # PostgreSQL password
        "passwd" : "R5wM9N5qCEU3an#&rku8mxrVBuF@ur", # MySQL password
        "db" : "uspto",
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

    # Build list of links
    args['link_list'] = get_link_list(args)
    # Download and process each link into database
    process_all_links(args)
