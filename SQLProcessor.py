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
from pprint import pprint

class SQLProcess:

    # Initialize connection to database using arguments
    def __init__(self, db_args):

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

    # Load the insert query into the database
    def load(self, sql, args):

        # Include logger
        logger = NIBRSLogger.logging.getLogger("NIBRS_Database_Construction")

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
    def load_csv_bulk_data(self, args, data_type, csv_file_obj):

        # Set the start time
        start_time = time.time()

        logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")
        print('[Staring to load csv files in bulk to ' + args['database_type'] + ']')
        logger.info('Staring to load csv files in bulk to ' + args['database_type'])

        # Connect to database if not connected
        if self._conn == None:
            self.connect()

        if "table_name" in csv_file_obj:
            # Print message to stdout and log about which table is being inserted
            print("Database bulk load query started for: " + data_type + " from filename: " + csv_file_obj['csv_file_name'])
            logger.info("Database bulk load query started for: " + data_type + " from filename: " + csv_file_obj['csv_file_name'])

            # If postgresql build query
            if self.database_type == "postgresql":

                # Set flag to determine if the query was successful
                bulk_insert_successful = False
                bulk_insert_failed_attempts = 0
                # Loop until successfull insertion
                while bulk_insert_successful == False:

                    try:
                        sql = "COPY uspto." + csv_file_obj['table_name'] + " FROM STDIN DELIMITER '|' CSV HEADER"
                        #self._cursor.copy_from(open(csv_file['csv_file_name'], "r"), csv_file['table_name'], sep = "|", null = "")
                        self._cursor.copy_expert(sql, open(csv_file_obj['csv_file_name'], "r"))
                        # Return a successfull insertion flag
                        bulk_insert_successful = True

                    except Exception as e:
                        # Roll back the transaction
                        self._conn.rollback()
                        # Increment the failed counter
                        bulk_insert_failed_attempts += 1
                        print("Database bulk load query failed... " + csv_file_obj['csv_file_name'] + " into table: " + csv_file_obj['table_name'])
                        logger.error("Database bulk load query failed..." + csv_file_obj['csv_file_name'] + " into table: " + csv_file_obj['table_name'])
                        print("Query string: " + sql)
                        logger.error("Query string: " + sql)
                        traceback.print_exc()
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                        # If the cause was a dupllicate entry error, then try to clean the file
                        traceback_array = traceback.format_exc().splitlines()
                        for line in traceback_array:
                            if "duplicate key" in line:
                                # Insert the csv file item by item
                                #self.insert_csv_item_by_item(csv_file_obj['csv_file_name'], args)
                                # Remove the offending line from csv file
                                self.remove_item_from_csv(traceback_array, csv_file_obj['csv_file_name'], "duplicate_key_violation")
                            elif "violates not-null constraint" in line:
                                # Remove the offending line from csv file
                                self.remove_item_from_csv(traceback_array, csv_file_obj['csv_file_name'], "not_null_violation")


                        # Return a unsucessful flag
                        if bulk_insert_failed_attempts > 20:
                            return False

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
                        bulk_insert_sql = "LOAD DATA LOCAL INFILE '" + csv_file_obj['csv_file_name'] + "' INTO TABLE " + csv_file_obj['table_name'] + " FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' IGNORE 1 LINES"
                        bulk_insert_sql = bulk_insert_sql.replace("\\", "/")

                        # Execute the query built above
                        self._cursor.execute(bulk_insert_sql)
                        # Return a successfull insertion flag
                        bulk_insert_successful = True

                    except Exception as e:

                        # Increment the failed counter
                        bulk_insert_failed_attempts += 1
                        print("Database bulk load query attempt " + str(bulk_insert_failed_attempts) + " failed... " + csv_file_obj['csv_file_name'] + " into table: " + csv_file_obj['table_name'])
                        logger.error("Database bulk load query attempt " + str(bulk_insert_failed_attempts) + " failed..." + csv_file_obj['csv_file_name'] + " into table: " + csv_file_obj['table_name'])
                        print("Query string: " + bulk_insert_sql)
                        logger.error("Query string: " + bulk_insert_sql)
                        traceback.print_exc()
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())
                        # Return a unsucessful flag
                        if bulk_insert_failed_attempts > 8:
                            return False

        # Return a successfull message from the database query insert.
        return True

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

        logger = USPTOLogger.logging.getLogger("USPTO_Database_Construction")
        print("[Checking database for previous attempt to process the " + call_type + " file: " + file_name + "...]")
        logger.info("[Checking database for previous attempt to process the " + call_type + " file:" + file_name + "...]")

        # Connect to database if not connected
        if self._conn == None:
            self.connect()

        # Set the table_name
        table_name = "STARTED_FILES"

        # Build query to check the STARTED_FILES table to see if this file has been started already.
        check_file_started_sql = "SELECT COUNT(*) as count FROM uspto." + table_name + " WHERE FileName = '" + file_name + "' LIMIT 1"

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

            print("Database check if " + call_type + " file started failed... " + file_name + " from table: uspto.STARTED_FILES")
            logger.error("Database check if " + call_type + " file started failed... " + file_name + " from table: uspto.STARTED_FILES")
            traceback.print_exc()
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())

        # If no previous attempts to process the file have been found
        if check_file_started[0] == 0:
            # Insert the file_name into the table keeping track of STARTED_FILES
            if self.database_type == "postgresql":
                insert_file_started_sql = "INSERT INTO uspto." + table_name + "  (FileName) VALUES($$" + file_name + "$$)"
            elif self.database_type == "mysql":
                insert_file_started_sql = "INSERT INTO uspto." + table_name + " (FileName) VALUES('" + file_name + "')"

            print("No previous attempt found to process the " + call_type + " file: " + file_name + " in table: uspto.STARTED_FILES")
            logger.info("No previous attempt found to process the " + call_type + " file:" + file_name + " in table: uspto.STARTED_FILES")

            # Insert the record into the database that the file has been started.
            try:
                self._cursor.execute(insert_file_started_sql)

            except Exception as e:
                # If there is an error and using databse postgresql
                # Then rollback the commit??
                if self.database_type == "postgresql":
                    self._conn.rollback()
                print("Database check for previous attempt to process " + call_type + " file failed... " + file_name + " into table: uspto.STARTED_FILES")
                logger.error("Database check for previous attempt to process " + call_type + " file failed... " + file_name + " into table: uspto.STARTED_FILES")
                traceback.print_exc()
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                logger.error("Exception: " + str(exc_type) + " in Filename: " + str(fname) + " on Line: " + str(exc_tb.tb_lineno) + " Traceback: " + traceback.format_exc())


        # If the file was found in the STARTED_FILES table,
        # delete all the records of that file in all tables.
        elif check_file_started[0] != 0:

            print("Found previous attempt to process the " + call_type + " file: " + file_name + " in table: uspto.STARTED_FILES")
            logger.info("Found previous attempt to process the " + call_type + " file:" + file_name + " in table: uspto.STARTED_FILES")

            # Build array to hold all table names to have
            # records deleted for patent grants
            if call_type == "grant":
                table_name_array = [
                    "GRANT",
                    "INTCLASS_G",
                    "CPCCLASS_G",
                    "USCLASS_G",
                    "INVENTOR_G",
                    "AGENT_G",
                    "ASSIGNEE_G",
                    "APPLICANT_G",
                    "NONPATCIT_G",
                    "EXAMINER_G",
                    "GRACIT_G",
                    "FORPATCIT_G",
                    "FOREIGNPRIORITY_G"
                ]
            # Records deleted for patent applications
            elif call_type == "application":
                table_name_array = [
                    "APPLICATION",
                    "INTCLASS_A",
                    "USCLASS_A",
                    "CPCCLASS_A",
                    "FOREIGNPRIORITY_A",
                    "AGENT_A",
                    "ASSIGNEE_A",
                    "INVENTOR_A",
                    "APPLICANT_A"
                ]

            # Records deleted for PAIR data
            elif call_type == "PAIR":
                table_name_array = [
                    "TRANSACTION_P",
                    "ADJUSTMENT_P",
                    "ADJUSTMENTDESC_P",
                    "CORRESPONDENCE_P",
                    "CONTINUITYCHILD_P",
                    "CONTINUITYPARENT_P",
                    "EXTENSION_P",
                    "EXTENSIONDESC_P"
                ]

            # Records deleted for classifcation data
            elif call_type == "class":
                table_name_array = [
                    "USCLASS_C",
                    "CPCCLASS_C",
                    "USCPC_C",
                ]

            # Records deleted for patent litigation data
            elif call_type == "legal":
                table_name_array = [
                    "CASE_L",
                    "PATENT_L",
                    "ATTORNEY_L",
                    "PARTY_L"
                ]

            print("Starting to remove previous attempt to process the " + call_type + " file: " + file_name + " in table: uspto.STARTED_FILES")
            logger.info("Starting to remove previous attempt to process the " + call_type + " file:" + file_name + " in table: uspto.STARTED_FILES")

            # Loop through each table_name defined by call_type
            for table_name in table_name_array:

                # Build the SQL query here
                remove_previous_record_sql = "DELETE FROM uspto." + table_name + " WHERE FileName = '" + file_name + "'"

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