#!/usr/bin/env python
# -*- coding: utf-8 -*-
# NIBRS SQLProcessor
# Author: Joseph Lee
# Email: joseph@ripplesoftware.ca
# Website: www.ripplesoftware.ca
# Github: www.github.com/rippledj/NIBRS

import MySQLdb
import psycopg2
import traceback
import time
import sys
import os
import csv
from pprint import pprint
import NIBRSLogger

class SQLProcess:

    # Initialize connection to database using arguments
    def __init__(self, db_args, args):

        # Pass the database type to class variable
        self.database_type = db_args['database_type']

        # Define class variables
        self._host = db_args['host']
        self._port = db_args['port']
        self._username = db_args['user']
        self._password = db_args['passwd']
        self._dbname = db_args['db']
        self._charset = db_args['charset']
        self._conn = None
        self._cursor = None

        # Track whether data tables have been inserted or not
        self.inserted_code_tables = []
        self.inserted_agency_tables = []
        # Get a list of table names
        self.agency_table_names = self.get_agency_table_names(args)
        self.main_table_names = self.get_main_table_names(args)
        self.code_table_names = self.get_code_table_names(args)
        self.primary_key_list = self.get_primary_key_list(args)

    # Establish connection to the database
    def connect(self):

        logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")

        # Connect to MySQL
        if self.database_type == "mysql":

            try:
                if self._conn == None:
                    self._conn = MySQLdb.connect(
                        host = self._host,
                        user = self._username,
                        passwd = self._password,
                        db = self._dbname,
                        port = self._port,
                        charset = self._charset
                    )
                    print("Connection to MySQL database established.")
                    logger.info("Connection to MySQL database established.")

                if self._cursor == None:
                    self._cursor = self._conn.cursor()
                    self._cursor.connection.autocommit(True)
            except:
                traceback.print_exc()
                exit()

        # Connect to PostgreSQL
        if self.database_type == "postgresql":

            if self._conn == None:
                # Get a connection, if a connect cannot be made an exception will be raised here
                self._conn = psycopg2.connect("host=" + self._host +  " dbname=" + self._dbname + " user=" + self._username + " password=" + self._password + " port=" + str(self._port))
                self._conn.autocommit = True

            if self._cursor == None:
                # conn.cursor will return a cursor object, you can use this cursor to perform queries
                self._cursor = self._conn.cursor()
                print("Connection to PostgreSQL database established.")
                logger.info("Connection to PostgreSQL database established.")

            # Get a list of all tables available in the database
            table_list = self.get_list_of_all_tables();


    # Get a list of all tables in the NIBRS database
    def get_list_of_all_tables(self):

        # Set process time
        start_time = time.time()

        logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
        print("[Checking database for list of all tables...]")
        logger.info("[Checking database for list of all tables...]")

        # Connect to database if not connected
        if self._conn == None:
            self.connect()

        # Execute the query to check if file has been stared before
        try:
            # If using PostgreSQL
            if self.database_type == "postgresql":
                # Build query to get list of all tables in NIBRS database
                get_table_list_sql = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"
                self._cursor.execute(get_table_list_sql)
                # Check the count is true or false.
                table_list = self._cursor.fetchall()
                # Print list of tables found
                for item in table_list:
                    print("--  Table Found: " + item[1])
            # MySQL
            elif self.database_type == "mysql":
                # Build query to get list of all tables in NIBRS database
                get_table_list_sql = "SHOW TABLES"
                self._cursor.execute(get_table_list_sql)
                # Check the count is true or false.
                table_list = self._cursor.fetchall()
                # Print list of tables found
                for item in table_list:
                    print("--  Table Found: " + item[1])

        except Exception as e:
            # If there is an error and using databse postgresql
            # Then rollback the commit??
            if self.database_type == "postgresql":
                self._conn.rollback()

            # Print and log general fail comment
            print("Database check for table list failed...")
            logger.error("Database check for table list failed")
            traceback.print_exc()
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


    # Load the insert query into the database
    def load(self, sql, args):

        # Include logger
        logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")

        # Get a list of table names
        agency_table_names = self.get_agency_table_names(args)
        code_table_names = self.get_code_table_names(args)
        main_table_names = self.get_main_table_names(args)

        # Connect to database if not connected
        if self._conn == None:
            self.connect()

        # Execute the query passed into funtion
        try:
            self._cursor.execute(sql)
            #self._conn.commit()
        except Exception as e:
            # If there is an error and using databse postgresql
            # Then rollback the commit??
            if self.database_type == "postgresql":
                self._conn.rollback()

            print("Database INSERT query failed... " + args['file_name'] + " into table: " + args['table_name'] + " Document ID Number " + args['document_id'])
            logger.error("Database INSERT query failed..." + args['file_name'] + " into table: " + args['table_name'] + " Document ID Number " + args['document_id'])
            print("Query string: " + sql)
            logger.error("Query string: " + sql)
            traceback.print_exc()
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


    # This function accepts an array of csv files which need to be inserted
    # using COPY command in postgresql and LOAD INFILE in MySQL
    def load_csv_bulk_data(self, item, args):

        # Set the start time
        start_time = time.time()

        logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")

        # Extract the table name and change to lower-case
        item['table_name'] = item['csv_filename'].split(".")[0]
        item['table_name'] = item['table_name'].lower()
        # Get the encoding scheme for the particular csv file
        encoding = self.set_file_encoding(item['csv_filename'], item['encoding'])


        # If postgresql build query
        if self.database_type == "postgresql":

            # Check that table name is in all table names
            if (item['table_name'] in self.agency_table_names or item['table_name'] in self.main_table_names or item['table_name'] in self.code_table_names) and (item['table_name'] not in self.inserted_code_tables and item['table_name'] not in self.inserted_agency_tables):

                # Print message to stdout and log about which table is being inserted
                print("** Database bulk load query started for " + item['base_filename'] + ": "  + item['table_name']  + " - " + item['csv_filename'])
                logger.info("** Database bulk load query started for " + item['base_filename'] + ": "  + item['table_name'] + " - " + item['csv_filename'])

                # Connect to database if not connected
                if self._conn == None:
                    self.connect()

                # Set flag to determine if the query was successful
                bulk_insert_successful = False
                bulk_insert_failed_attempts = 0
                allowed_insert_attempts = 1
                # Loop until successfull insertion
                while bulk_insert_successful == False:

                    try:

                        # Turn off the foreign key checks
                        sql = "ALTER TABLE " + item['table_name'] + " DISABLE TRIGGER ALL"
                        self._cursor.execute(sql)

                        # Build the base SQL query
                        sql = "COPY nibrs." + item['table_name'] + " FROM STDIN DELIMITER ',' CSV HEADER"
                        # Check for encoding
                        # TODO: Due to encoding conversion earlier in sanitizer, this check
                        # Should not be needed anymore
                        if encoding is not None:
                            sql = sql + " encoding '" + encoding + "'"
                        #self._cursor.copy_from(open(csv_file['csv_file_name'], "r"), csv_file['table_name'], sep = "|", null = "")
                        self._cursor.copy_expert(sql, open(item['csv_full_filepath'], "r"))

                        # Turn on the foreign key checks
                        sql = "ALTER TABLE " + item['table_name'] + " ENABLE TRIGGER ALL"
                        self._cursor.execute(sql)

                        # Print message to stdout and log
                        print("** SUCCESS ** Database bulk load query succeeded for: " + item['base_filename'] + " inserted to data tables: "  + item['table_name']  + " - " + item['csv_filename'])
                        logger.info("** SUCCESS ** Database bulk load query succeeded for: " + item['base_filename'] + " inserted to data tables: "  + item['table_name'] + " - " + item['csv_filename'])

                        # If table was data table, add the table name to inserted data tables
                        # NOTE: This needs to be fixed because different states have different agencies,
                        # although some overlap from year file to year file
                        if item['table_name'] in self.code_table_names and item['table_name'] not in self.inserted_code_tables:
                            self.inserted_code_tables.append(item['table_name'])
                            # Print message to stdout and log
                            print("** Code table for " + item['base_filename'] + " inserted to code tables list: "  + item['table_name']  + " - " + item['csv_filename'])
                            logger.info("** Code table for " + item['base_filename'] + " inserted to code tables list: "  + item['table_name'] + " - " + item['csv_filename'])

                        # Return success
                        return True

                    except Exception as e:
                        # Roll back the transaction
                        self._conn.rollback()

                        # Increment the failed counter
                        bulk_insert_failed_attempts += 1
                        print("** FAILED ** Database item by item load query failed for " + item['base_filename'] + "... " + item['csv_filename'] + " into table: " + item['table_name'])
                        logger.error("** FAILED ** Database item by item load query failed for " + item['base_filename'] + "... " + item['csv_filename'] + " into table: " + item['table_name'])
                        print("** Query string: " + sql)
                        logger.error("** Query string: " + sql)
                        traceback.print_exc()
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                        # If the cause was a dupllicate entry error, then try to clean the file
                        traceback_array = traceback.format_exc().splitlines()

                        # Return a unsucessful flag
                        if bulk_insert_failed_attempts > allowed_insert_attempts:
                            return False

            # If there was a reason not to insert the file to the database
            else:
                # If the code table has been inserted already
                # look for unique records only and attempt insert
                if item['table_name'] in self.inserted_code_tables or item['table_name'] in self.inserted_agency_tables:

                    # Print message to stdout and log that table will be checked for new records
                    print("** Code or agency table already inserted into database: " + item['table_name'] + " - " + item['csv_filename'])
                    logger.info("** Code or agency table already inserted into database: " + item['table_name'] + " - " + item['csv_filename'])

                    try:
                        # Insert the items at at a time
                        print("** Checking .csv file for new unique values: " + item['base_filename'] + "... " + item['csv_filename'] + " into table: " + item['table_name'])
                        logger.error("** Checking .csv file for new unique values: " + item['base_filename'] + "... " + item['csv_filename'] + " into table: " + item['table_name'])

                        # Load the csv_file into list of dictionary
                        with open(item['csv_filename'], "r") as infile:
                            csv_obj = [{k: int(v) for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]

                        new_records = []
                        for line in csv_obj:
                            # Check the association of the primary key column for that table
                            pk = get_primary_key_column_for_table(item['table_name'])
                            # Check if the value for the primary key value is inseted into table already
                            record_exists = check_primary_value_exists(line, pk, item['table_name'], args)
                            # If record not in database, add to list to be inserted
                            if not record_exists:
                                new_records.append(csv_ojb)

                        # Write the new unique values into the .csv file and bulk .csv insert
                        if len(new_records) != 0:

                            # Re-create the .csv file with only unique records
                            write_new_unique_to_csv(new_records, item, args)

                            # Turn off the foreign key checks
                            sql = "ALTER TABLE " + item['table_name'] + " DISABLE TRIGGER ALL"
                            self._cursor.execute(sql)

                            # Build the base SQL query
                            sql = "COPY nibrs." + item['table_name'] + " FROM STDIN DELIMITER ',' CSV HEADER"
                            # Check for encoding
                            # TODO: Due to encoding conversion earlier in sanitizer, this check
                            # Should not be needed anymore
                            if encoding is not None:
                                sql = sql + " encoding '" + encoding + "'"
                            #self._cursor.copy_from(open(csv_file['csv_file_name'], "r"), csv_file['table_name'], sep = "|", null = "")
                            self._cursor.copy_expert(sql, open(item['csv_full_filepath'], "r"))

                            # Turn on the foreign key checks
                            sql = "ALTER TABLE " + item['table_name'] + " ENABLE TRIGGER ALL"
                            self._cursor.execute(sql)

                            # Print message to stdout and log
                            print("** SUCCESS ** Database bulk load query of only unique values succeeded for: " + item['base_filename'] + " inserted to data tables: "  + item['table_name']  + " - " + item['csv_filename'])
                            logger.info("** SUCCESS ** Database bulk load query of only unique values succeeded for: " + item['base_filename'] + " inserted to data tables: "  + item['table_name'] + " - " + item['csv_filename'])

                    except Exception as e:
                        # Roll back the transaction
                        self._conn.rollback()

                        # Increment the failed counter
                        bulk_insert_failed_attempts += 1
                        print("** FAILED ** Database item by item load query failed for " + item['base_filename'] + "... " + item['csv_filename'] + " into table: " + item['table_name'])
                        logger.error("** FAILED ** Database item by item load query failed for " + item['base_filename'] + "... " + item['csv_filename'] + " into table: " + item['table_name'])
                        print("** Query string: " + sql)
                        logger.error("** Query string: " + sql)
                        traceback.print_exc()
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                        # If the cause was a dupllicate entry error, then try to clean the file
                        traceback_array = traceback.format_exc().splitlines()

                        # Return a unsucessful flag
                        if bulk_insert_failed_attempts > allowed_insert_attempts:
                            return False

                # If the table name is unknown
                elif item['table_name'] not in self.agency_table_names or item['table_name'] not in self.main_table_names or item['table_name'] not in self.code_table_names:
                    # Print message to stdout and log about which table is being inserted
                    print("Unknown table name attempted to insert into database: " + item['table_name'] + " - " + item['csv_filename'])
                    logger.error("Unknown table name attempted to insert into database: " + item['table_name'] + " - " + item['csv_filename'])

            # Return a successfull message from the database query insert.
            return True

        # If MySQL build query
        elif self.database_type == "mysql":

                # Set flag to determine if the query was successful
                bulk_insert_successful = False
                bulk_insert_failed_attempts = 0
                # Loop until the file was successfully deleted
                # NOTE : Used because MySQL has table lock errors
                while bulk_insert_successful == False:

                    try:
                        # TODO: consider "SET foreign_key_checks = 0" to ignore
                        # TODO: LOCAL is used to set duplicate key to warning instead of error
                        # TODO: IGNORE is also used to ignore rows that violate duplicate unique key constraints
                        bulk_insert_sql = "LOAD DATA LOCAL INFILE '" + item['csv_full_filepath'] + "' INTO TABLE " + item['table_name'] + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES"
                        bulk_insert_sql = bulk_insert_sql.replace("\\", "/")

                        # Execute the query built above
                        self._cursor.execute(bulk_insert_sql)
                        # Return a successfull insertion flag
                        bulk_insert_successful = True

                        return True

                    except Exception as e:
                        # Increment the failed counter
                        bulk_insert_failed_attempts += 1
                        print("** Database bulk load query attempt " + str(bulk_insert_failed_attempts) + " failed... " + csv_file_obj['csv_file_name'] + " into table: " + csv_file_obj['table_name'])
                        logger.error("** Database bulk load query attempt " + str(bulk_insert_failed_attempts) + " failed..." + csv_file_obj['csv_file_name'] + " into table: " + csv_file_obj['table_name'])
                        print("** Query string: " + bulk_insert_sql)
                        logger.error("** Query string: " + bulk_insert_sql)
                        traceback.print_exc()
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("** Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                        # Return a unsucessful flag
                        if bulk_insert_failed_attempts > 8:
                            return False


    # Used to retrieve ID by matching fields of values
    def query(self,sql):
        #try:
        if self._conn == None:
            self.connect()
            self._cursor.execute(sql)
            #self._conn.commit()
            result = self._cursor.fetchone()
            return int(result[0])
        else:
            self._cursor.execute(sql)
            #self._conn.commit()
            result = self._cursor.fetchone()
            return int(result[0])
        #finally:
            #self.close()

    # Used to remove records from database when a file previously
    # started being processed and did not finish. (when insert duplicate ID error happens)
    def remove_previous_file_records(self, call_type, file_name):

        # Set process time
        start_time = time.time()

        logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
        print("[Checking database for previous attempt to process the " + call_type + " file: " + file_name + "...]")
        logger.info("[Checking database for previous attempt to process the " + call_type + " file:" + file_name + "...]")

        # Connect to database if not connected
        if self._conn == None:
            self.connect()

        # Set the table_name
        table_name = "STARTED_FILES"

        # Build query to check the STARTED_FILES table to see if this file has been started already.
        check_file_started_sql = "SELECT COUNT(*) as count FROM NIBRS." + table_name + " WHERE FileName = '" + file_name + "' LIMIT 1"

        # Execute the query to check if file has been stared before
        try:
            self._cursor.execute(check_file_started_sql)
            # Check the count is true or false.
            check_file_started = self._cursor.fetchone()

        except Exception as e:
            # Set the variable and automatically check if database records exist
            check_file_started = True
            # If there is an error and using databse postgresql
            # Then rollback the commit??
            if self.database_type == "postgresql":
                self._conn.rollback()

            print("Database check if " + call_type + " file started failed... " + file_name + " from table: NIBRS.STARTED_FILES")
            logger.error("Database check if " + call_type + " file started failed... " + file_name + " from table: NIBRS.STARTED_FILES")
            traceback.print_exc()
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())

        # If no previous attempts to process the file have been found
        if check_file_started[0] == 0:
            # Insert the file_name into the table keeping track of STARTED_FILES
            if self.database_type == "postgresql":
                insert_file_started_sql = "INSERT INTO NIBRS." + table_name + "  (FileName) VALUES($$" + file_name + "$$)"
            elif self.database_type == "mysql":
                insert_file_started_sql = "INSERT INTO NIBRS." + table_name + " (FileName) VALUES('" + file_name + "')"

            print("No previous attempt found to process the " + call_type + " file: " + file_name + " in table: NIBRS.STARTED_FILES")
            logger.info("No previous attempt found to process the " + call_type + " file:" + file_name + " in table: NIBRS.STARTED_FILES")

            # Insert the record into the database that the file has been started.
            try:
                self._cursor.execute(insert_file_started_sql)

            except Exception as e:
                # If there is an error and using databse postgresql
                # Then rollback the commit??
                if self.database_type == "postgresql":
                    self._conn.rollback()
                print("Database check for previous attempt to process " + call_type + " file failed... " + file_name + " into table: NIBRS.STARTED_FILES")
                logger.error("Database check for previous attempt to process " + call_type + " file failed... " + file_name + " into table: NIBRS.STARTED_FILES")
                traceback.print_exc()
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


        # If the file was found in the STARTED_FILES table,
        # delete all the records of that file in all tables.
        elif check_file_started[0] != 0:

            print("Found previous attempt to process the " + call_type + " file: " + file_name + " in table: NIBRS.STARTED_FILES")
            logger.info("Found previous attempt to process the " + call_type + " file:" + file_name + " in table: NIBRS.STARTED_FILES")

            # Build array to hold all table names to have
            # records deleted for patent grants
            if call_type == "grant":
                table_name_array = [
                ]

            # Records deleted for patent litigation data
            elif call_type == "legal":
                table_name_array = [
                ]

            print("Starting to remove previous attempt to process the " + call_type + " file: " + file_name + " in table: NIBRS.STARTED_FILES")
            logger.info("Starting to remove previous attempt to process the " + call_type + " file:" + file_name + " in table: NIBRS.STARTED_FILES")

            # Loop through each table_name defined by call_type
            for table_name in table_name_array:

                # Build the SQL query here
                remove_previous_record_sql = "DELETE FROM NIBRS." + table_name + " WHERE FileName = '" + file_name + "'"

                # Set flag to determine if the query was successful
                records_deleted = False
                records_deleted_failed_attempts = 1
                # Loop until the file was successfully deleted
                # NOTE : Used because MySQL has table lock errors
                while records_deleted == False and records_deleted_failed_attempts < 10:
                    # Execute the query pass into funtion
                    try:
                        self._cursor.execute(remove_previous_record_sql)
                        records_deleted = True
                        #TODO: check the numer of records deleted from each table and log/print
                        # Print and log finished check for previous attempt to process file
                        print("Finished database delete of previous attempt to process the " + call_type + " file: " + file_name + " table: " + table_name)
                        logger.info("Finished database delete of previous attempt to process the " + call_type + " file:" + file_name + " table: " + table_name)

                    except Exception as e:

                        # If there is an error and using databse postgresql
                        # Then rollback the commit??
                        if self.database_type == "postgresql":
                            self._conn.rollback()
                        print("Database delete attempt " + str(records_deleted_failed_attempts) + " failed... " + file_name + " from table: " + table_name)
                        logger.error("Database delete attempt " + str(records_deleted_failed_attempts) + " failed..." + file_name + " from table: " + table_name)
                        # Increment the failed attempts
                        records_deleted_failed_attempts += 1
                        traceback.print_exc()
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


    # Get main table names
    def get_main_table_names(self, args):

        table_names = []
        with open(args['main_table_names_file'], "r") as infile:
            for line in infile:
                table_names.append(line.replace('\n', ''))
        #print(main_table_names)
        return table_names


    # Get code table names
    def get_code_table_names(self, args):

        table_names = []
        with open(args['code_table_names_file'], "r") as infile:
            for line in infile:
                table_names.append(line.replace('\n', ''))
        #print(code_table_names)
        return table_names


    # Get agency table names
    def get_agency_table_names(self, args):

        table_names = []
        with open(args['agency_table_names_file'], "r") as infile:
            for line in infile:
                table_names.append(line.replace('\n', ''))
        #print(code_table_names)
        return table_names

    # Retrieves the encoding scheme if any from the list
    def set_file_encoding(self, csv_filename, encoding_list):
        # If no encoding return None
        if encoding_list == None: return None
        # If some items in encoding
        else:
            # Include logger
            logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")
            if csv_filename in encoding_list:
                encoding = encoding_list[csv_filename][encoding]
                logger.info("-- Found encoding scheme " + encoding + " for .csv file: " + csv_filename)
                print("-- Found encoding scheme " + encoding + " for .csv file: " + csv_filename)
                return encoding
            else: return None

    # Get the primary key definitions list
    def get_primary_key_list(self, args):
        primary_key_list = {}
        with open(args['primary_key_list_filename'], "r") as infile:
            for line in infile:
                line = line.strip().split(",")
                table_name = line.pop(0)
                primary_key_list[table_name] =
        #print(primary_key_list)
        return primary_key_list

    # Get the the primary key for a table by table name
    def get_primary_key_column_for_table(self, table_name):
        # Look for the table name in primary key list
        if table_name in self.primary_key_list:
            keys = self.primary_key_list[table_name]
            # Check if the key is single column
            if len(keys) == 1: return keys[0]
            # Check if the key multiple columns
            elif len(keys) == 2: return keys[0] + "-" + keys[1]
        else: return None

    # Check if a value exists for the row
    def check_primary_value_exists(self, line, pk, table_name, args):
        # Prepare the query
        sql = "SELECT " + pk + " FROM " + table_name + " WHERE " + pk + " = '" + line[pk] + "'"
        # Execute the query
        self._cursor.execute(sql)
        # Check the count is true or false.
        result = self._cursor.fetchone()
        # Return the exists status
        if result[0] == 0: return False
        else: return True

    # Write the new unique records back to original csv file
    def write_new_unique_to_csv(self, new_records, item, args):
        # Get the keys for header
        keys = new_records[0].keys()
        # Write list items to .csv file
        with open(item['extract_directory'] + item['csv_filename'], 'w') as outfile:
            writer = csv.DictWriter(outfile, keys)
            writer.writeheader()
            writer.writerows(new_records)
