# NIBRSSanitizer.py
# NIBRS Bulk Data Parser - Sanitizes the .csv files for database
# Description: Sanitizes the .csv files for database.
# Author: Joseph Lee
# Email: joseph@ripplesoftware.ca
# Website: www.ripplesoftware.ca
# Github: www.github.com/rippledj/fbi-nibrs

# Import Python Modules
import logging
import string
import traceback
import time
import os
import sys
from pprint import pprint
import NIBRSLogger

# Accepts directory paths to sanitize incoming .csv file data
def sanitize_csv_files(item, args):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
    logger.info("-- Starting to sanitize list of .csv files for item: " + item['zip_filename'] + "...")
    print("-- Starting to sanitize list of .csv files for item: " + item['zip_filename'] + "...")

    # Classify the data-type of the file
    if item['year'] < 2015: item['data_type'] = "old"
    else: item['data_type'] = "new"

    # Remove any duplicate columns from the .csv files in the item
    remove_duplicate_columns(item)

    # Fix the encoding for any tables that do not use UTF-8
    item['csv_file_encoding'] = get_file_encoding(item)

    # Loop through all the identified .csv files available
    for csv_file in item['csv_files']:


        # Check which column must be added to the .csv file using the filename
        adj_desc = get_adjustment_requirements(item, args)
        # If .csv file requires year column to be added
        if adj_desc != None:
            pass

        # Add the file_code column to the end of .csv
        add_file_code_column(item, args)

# Return a list of adjustments to be made to file
def get_adjustment_requirements(item, args):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
    # Open the file with requriements
    requirements_list = []
    with open(args['adjustment_req_filename'], "r") as infile:
        for line in infile:
            line = line.split(",")
            requirements_list.append(line)

# Remove any columns that are duplicated
def remove_duplicate_columns(item):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")

    for csv_file in item['csv_files']:

        logger.info("-- Removing duplicates from .csv file: " + csv_file)
        print("-- Removing duplicates from .csv file: " + csv_file)

        # Open file and get headers, remove '"' and split on ","
        with open(csv_file, "r") as infile:
            contents = infile.readlines()
            headers = contents[0].lower()
            headers = headers.replace('"',"").split(",")
        # Check for duplicates in headers list
        if len(headers) == len(set(headers)):

            logger.info("-- No duplicate columns found in .csv file: " + csv_file)
            print("-- No duplicate columns found in .csv file: " + csv_file)
        # If duplicates found in headers
        else:

            duplicates = []
            for item in headers:
                if headers.count(item) != 1:
                    # Add item to list of duplicates
                    duplicates.append(item)
            # Remove duplciate occurances of duplicates found
            dup_set = set(duplicates)
            # Get the position(s) of the duplicate columns
            positions = []
            for item in dup_set:
                item_found = False
                col_num = 0
                for col in headers:
                    # If column matches duplicate
                    if item == col:
                        # If not the first column that matched get position
                        if item_found:
                            logger.info("-- Duplicate column " + item + " found at position " + str(col_num) +  " in .csv file: " + csv_file)
                            print("-- Duplicate column " + item + " found at position " + str(col_num) +  " in .csv file: " + csv_file)
                            positions.append(col_num)
                        else: item_found = True
                    col_num += 1

            # Re-write the file and remove the duplicate columns
            with open(csv_file, "w") as outfile:

                logger.info("-- Re-writing in .csv file: " + csv_file)
                print("-- Re-writing in .csv file: " + csv_file)

                # Loop through the original contents
                line_num = 0
                for line in contents:

                    # Fix the column headers for '"' and remove capital letters
                    if line_num == 0:
                        line = line.lower().replace('"', "")
                    line_num += 1
                    # Split the .csv line
                    line = line.split(",")
                    # Create a temp line list
                    tmp_line = []
                    # Create line item index counter
                    line_index = 0
                    for item in line:
                        # If line item is not in the duplicate index list
                        # then append to tmp_line
                        if line_index not in positions:
                            tmp_line.append(item.strip())
                        # Increment the line index position
                        line_index += 1
                    # Put the line back together again
                    line = ",".join(tmp_line)
                    # Write the line back to file
                    outfile.write(line + "\n")
            # Return success status
            return True



# Get file encoding
def get_file_encoding(item):

    # Include logger
    logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")

    special_encoding = []

    pg_load_filename = "postgres_load.sql"
    # Get the filepath of the PostgreSQL load file for the item
    if os.path.exists(item['extract_directory'] + pg_load_filename):
        pg_load_filename =item['extract_directory'] + pg_load_filename
        logger.info("-- PostgreSQL load file found: " + pg_load_filename)
        print("-- PostgreSQL load file found: " + pg_load_filename)
    elif os.path.exists(item['extract_directory'] + item['state_code'] + "/" +  pg_load_filename):
        pg_load_filename = item['extract_directory'] + pg_load_filename
        logger.info("-- PostgreSQL load file found: " + pg_load_filename)
        print("-- PostgreSQL load file found: " + pg_load_filename)
    else:
        logger.info("-- No PostgreSQL load file found: " + item['base_filename'])
        print("-- No PostgreSQL load file found for item: " + item['base_filename'])
        return False

    # Open the file and get any special table encoding specified in the COPY commands
    with open(pg_load_filename, "r") as infile:
        for line in infile:
            line = line.strip()
            line_lower = line.lower()
            # If the line is a copy command
            if line_lower.startswith("\\copy") and "encoding" in line_lower:
                # Get the string for the encoding type
                encoding = line_lower.split("encoding")[-1]
                encoding = encoding.strip().replace("'", "").replace(";", "")
                # Get the table name and .csv file that uses the special encoding
                table_name = line_lower.split()[1]
                file_name = line.split()[3].replace("'", "")
                # Append to list
                special_encoding.append({ "file_name" : file_name, "table_name" : table_name, "encoding" : encoding})

                # Log the special enocding found
                logger.info("-- Special encoding - " + encoding + " - found for file: " + file_name + " to table: " + table_name)
                print("-- Special encoding - " + encoding + " - found for file: " + file_name + " to table: " + table_name)


    # Return the list of special encoded tables
    return special_encoding



# Check the functions
if __name__ == "__main__":

    #item = {}
    #item['csv_files'] = [
        #"/Users/development/Software/NIBRS/TMP/downloads/AZ-2015/nibrs_month.csv",
        #"/Users/development/Software/NIBRS/TMP/downloads/AZ-2019/AZ/nibrs_month.csv"
    #]
    #remove_duplicate_columns(item)

    item = {}
    item['extract_directory'] = "/Users/development/Software/NIBRS/RES/installation/"
    item['state_code'] = "AZ"
    get_file_encoding(item)
