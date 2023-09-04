#! /bin/bash

# This script
# Extracts data from /etc/passwd file into a CSV file.

# The csv data file contains the user name, user id and 
# home directory of each user account defined in /etc/passwd

# Transforms the text delimiter from ":" to ",".
# Loads the data from the CSV file into a table in PostgreSQL database.

#1) Extract phase 
echo "Extracting data"

# Extract the columns 1 (user name), 2 (user id) and 
# 6 (home directory path) from /etc/passwd
cut -d":" -f1,3,6 /etc/passwd > extracted-data.txt

#2) Transform phase
echo "Transforming data"
# read the extracted data and replace the colons with commas.

cat extracted-data.txt | tr ":" "," > transformed-data.csv

#3) Extract phase
current_directory=$(pwd)
echo "\c template1;\COPY users  FROM '$current_directory/transformed-data.csv' DELIMITERS ',' CSV;" | \
psql --username=postgres --host=localhost -W
