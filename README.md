# **FBI - NIBRS DATA PARSER**

Copyright (c) 2021 Ripple Software. All rights reserved.

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

**Author:** Joseph Lee

**Email:** joseph@ripplesoftware.ca

**Website:** https://www.ripplesoftware.ca

**Github Repository:** https://github.com/rippledj/fbi-nibrs

## **Description:**
The script requires Python 3.6 or higher and will not work properly with Python 2.

The script is run from the command line and will populate a PostgreSQL or MySQL database with the FBI NIBRS data. It is recommended to use PostgreSQL since PG provides better performance over the large data-set.  The data is downloaded from an Amazon AWS instance run by the FBI.  The data files can be downloaded one at a time from https://www.fbi.gov/services/cjis/ucr/nibrs.

The usage of the script is outlined below:

## **Instructions:**
There are three steps.
1. Install the required database
2. Run the parser NIBRSparser.py

### 1. Install the database

Run the appropriate database creation scripts depending if you intend to store the NIBRS data in MySQL or PostgreSQL.  The script will create a user 'nibrs' and limit the scope of the user to the nibrs database. If you want to change the default password for the user, edit the appropriate .sql file before running it.  Also, some configuration of your database maybe necessary depending on the settings choose when running the script.  For example the ability to bulk insert CSV files are disabled by default in MySQL.

_MySQL or MariaDB_

RES/installation/mysql_setup.sql
RES/installation/mariadb_setup.sql

_PostgreSQL_

RES/installation/postgres_setup.sql

### 2. Run the parser

Before the NIBRSParser.py can run successfully, the database connection and authentication details must be added (if database storage will be specified). Text search for the phrase "# Database args" to find the location where database credentials must be changed. Enter "mysql" or "postgresql" as the database_type. Enter the port of your MySQL or PostgreSQL installation if you have a non-default port. If you changed the default password in the database creation file, then you should also change the password here.

The command to run the script is:

$ python NIBRSParser.py

### 3. Check the log files

The script will keep track of processed files in the **LOG** directory. There are log files for data files (**NIBRS_links.log**) and a main log file **NIBRS_app.log** which keeps track of errors and warnings from the script.  If the script crashes for any reason, you can simply start the script again and it will clear any partially processed data and start where it left off.  You can set the verbosity of the stdout and **NIBRS_app.log** logs with the 'log_level' and 'stdout_level' variables at the top of the main function.

### Contact

If you have questions about the NIBRS data you can contact:
Author, Joseph Lee: joseph@ripplesoftware.ca
