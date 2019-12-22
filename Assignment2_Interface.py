#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):

    #Set up connection to postgres
    cur = openconnection.cursor()

    #Count the number of range partitions
    cur.execute("SELECT COUNT(*) FROM RangeRatingsMetadata")
    range_partition_count = int(cur.fetchone()[0])
       #print range(range_partition_count)

    #Temp array to store data
    to_String = []

    #Performing Range Query
    for i in range(range_partition_count):
        to_String.append("SELECT 'rangeratingspart"
                         + str(i)
                         +"' AS tablename, userid, movieid, rating FROM rangeratingspart"
                         + str(i) + " WHERE rating >= "
                         + str(ratingMinValue)
                         + " AND rating <= "
                         + str(ratingMaxValue))
    #Count the number of Round Robin Parition
    cur.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata")
    rr_partition_count = int(cur.fetchone()[0])
       #print range(rr_partition_count)

    #Performing Round Robin Query
    for i in range(rr_partition_count):
        to_String.append("SELECT 'roundrobinratingspart"
                         + str(i)
                         + "' AS tablename, userid, movieid, rating FROM roundrobinratingspart"
                         + str(i)
                         + " WHERE rating >= "
                         + str(ratingMinValue)
                         + " AND rating <= "
                         + str(ratingMaxValue))

    #to_String processing
    to_String_Q = 'SELECT * FROM ({0}) AS T'.format(' UNION ALL '.join(to_String))
    to_String_FS = open(outputPath, 'w+')
    os.chmod(outputPath, 0o777) #Gives Highest Permission
    write_file = "COPY (" + to_String_Q + ") TO '" + os.path.abspath(to_String_FS.name) + "' (FORMAT text, DELIMITER ',')"

    #Clean up
    cur.execute(write_file)
    to_String_FS.close()
    cur.close()

def PointQuery(ratingValue, openconnection, outputPath):

    #Set up connection to postgres
    cur = openconnection.cursor()

    #Count the number of range partitions
    cur.execute("SELECT COUNT(*) FROM RangeRatingsMetadata")
    range_partition_count = int(cur.fetchone()[0])

    #Temp array to store data
    to_String = []

    #Performing Range Query
    for i in range(range_partition_count):
        to_String.append("SELECT 'rangeratingspart"
                         + str(i)
                         +"' AS tablename, userid, movieid, rating FROM rangeratingspart"
                         + str(i)
                         + " WHERE rating = "
                         + str(ratingValue))

    # Count the number of Round Robin Parition
    cur.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata")
    rr_partition_count = int(cur.fetchone()[0])
    for i in range(rr_partition_count):
        to_String.append("SELECT 'roundrobinratingspart"
                         + str(i)
                         + "' AS tablename, userid, movieid, rating FROM roundrobinratingspart"
                         + str(i)
                         + " WHERE rating = "
                         + str(ratingValue))

    to_String_Q = 'SELECT * FROM ({0}) AS T'.format(' UNION ALL '.join(to_String))
    to_String_FS = open(outputPath, 'w+')
    os.chmod(outputPath, 0o777)  # Gives Highest Permission
    write_file = "COPY (" + to_String_Q + ") TO '" + os.path.abspath(to_String_FS.name) + "' (FORMAT text, DELIMITER ',')"

    #Clean up
    cur.execute(write_file)
    to_String_FS.close()
    cur.close()
