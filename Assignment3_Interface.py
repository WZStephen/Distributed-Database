#!/usr/bin/python2.7
#
# Assignment3 Interface
#
import threading
import psycopg2
import os
import sys


##########################################Parallel Sort Begin#########################################################
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    try:
        # connect to postgres
        cur = openconnection.cursor()

        # Get the columns of input table
        cur.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable + "'")
        col_name = cur.fetchall()
        # print col_name

        range_max, range_min_val = part(InputTable, SortingColumnName, openconnection)

        # creating tables in range partition from 0, 1, 2, 3, 4
        for i in range(5):
            cur.execute("DROP TABLE IF EXISTS range_part" + str(i) + "")
            cur.execute("CREATE TABLE range_part" + str(i) + " (" + col_name[0][0] + " " + col_name[0][1] + ")")

            for j in range(1, len(col_name)):
                cur.execute("ALTER TABLE range_part" + str(i) + " ADD COLUMN " + col_name[j][0] + " " + col_name[j][1] + ";")

        # Creating threads
        thread = [0, 1, 2, 3, 4]
        for i in range(5):
            if i == 0:
                floor = range_min_val
                ceil = range_min_val + range_max
            else:
                floor = ceil
                ceil = ceil + range_max

            thread[i] = threading.Thread(target=range_insersion,args=(InputTable, SortingColumnName, i, floor, ceil, openconnection))

            # Starting Thread
            thread[i].start()

        for j in range(5):
            thread[i].join()
            # Aggregating the tables

        cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        # userid & integer
        cur.execute("CREATE TABLE " + OutputTable + " (" + col_name[0][0] + " " + col_name[0][1] + ")")

        for i in range(1, len(col_name)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + col_name[i][0] + " " + col_name[i][1] + ";")

        for i in range(5):
            cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + "range_part" + str(i) + "")

    except Exception as message:
        # Indicating error messages
        print "Exception Error is -->", message

    finally:
        # Drop all range partition tables
        for i in range(5):
            cur.execute("DROP TABLE IF EXISTS range_par" + str(i) + "")

        # cur.execute("select count(*) from " + OutputTable)
        # res = cur.fetchall()
        # print res

        #Printing out outputtable
        print "\nPrinting out OutputTable:"
        cur.execute("select * from " + OutputTable)
        res = cur.fetchall()
        print res

        # os.chmod("/home/user/Desktop/Assignment3/output1.csv", 0o777)  # Gives Highest Permission
        # cur.execute("COPY " + OutputTable + " TO '/home/user/Desktop/Assignment3/output1.csv'WITH(FORMAT CSV, HEADER)")
        # cur.close()

# Inserts in sorted value
def range_insersion(InputTable, col_n, key, floor, ceil, openconnection):
    cur = openconnection.cursor()

    table_name = "range_part" + str(key)

    # Check for minimum value of column
    if key != 0:
        cur.execute("INSERT INTO " + table_name + \
                    " SELECT * FROM " + InputTable + \
                    " WHERE " + col_n + " <= " + str(ceil) + \
                    " AND " + col_n + ">" + str(floor)+ \
                    " ORDER BY " + col_n + \
                    " ASC")
    else:
        cur.execute("INSERT INTO " + table_name + \
                    " SELECT * FROM " + InputTable + \
                    " WHERE " + col_n + " <= " + str(ceil) + \
                    " AND " + col_n + ">=" + str(floor) + \
                    " ORDER BY " + col_n + \
                    " ASC")

    # cur.close()

def part(InputTable, col_n, openconnection):
    # connect to postgres
    cur = openconnection.cursor()

    # Retrieve max from SortingColumnName
    cur.execute("SELECT MAX(" + col_n + ") FROM " + InputTable + "")
    max = cur.fetchone()
    range_max_val = (float)(max[0])

    # Retrieve min from SortingColumnName
    cur.execute("SELECT MIN(" + col_n + ") FROM " + InputTable + "")
    min = cur.fetchone()
    range_min_val = (float)(min[0])
    # print range_min_value

    range_max=(range_max_val - range_min_val) / 5

    return range_max, range_min_val



###########################################Parallel Join Begin #################################################################
def ParallelJoin(InputTable1, InputTable2, Table1Join, Table2Join, OutputTable, openconnection):
    try:
        # connect to postgres
        cur = openconnection.cursor()

        # Get columns of input tables
        cur.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable1 + "'")
        col_name1 = cur.fetchall()

        cur.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable2 + "'")
        col_name2 = cur.fetchall()

        range_max, range_min_value = part1(InputTable1, InputTable2, Table1Join, Table2Join, openconnection)

        # creating OutputTable
        cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        cur.execute("CREATE TABLE " + OutputTable + " (" + col_name1[0][0] + " " + col_name1[0][1] + ")")

        for i in range(1, len(col_name1)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + col_name1[i][0] + " " + col_name1[i][1] + ";")

        for i in range(len(col_name2)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + col_name2[i][0] + "1" + " " + col_name2[i][1] + ";")

        # Calls the OuputRangeTable function for temporary output range table
        range_query(InputTable1, InputTable2, Table1Join, Table2Join, col_name1, col_name2, range_max,
                    range_min_value, openconnection)

        # Creats threads
        thread = [0, 1, 2, 3, 4]
        for i in range(5):
            thread[i] = threading.Thread(target=range_query_insersion,args=(Table1Join, Table2Join, i, openconnection))
            thread[i].start()
        for j in range(5):
            thread[i].join()

        # Inserts in output table
        for i in range(5):
            cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM output_table" + str(i))

    except Exception as message:
        # Indicating the error message
        print "Exception Error is -->", message

    finally:
        # Drop all temp tables
        for i in range(5):
            cur.execute("DROP TABLE IF EXISTS output_table" + str(i))
            cur.execute("DROP TABLE IF EXISTS table1_range" + str(i))
            cur.execute("DROP TABLE IF EXISTS table2_range" + str(i))

        # cur.execute("select count(*) from " + OutputTable)
        # res = cur.fetchall()
        # print res

        #Printing out outputtable
        print "\nPrinting out OutputTable:"
        cur.execute("select * from " + OutputTable)
        res = cur.fetchall()
        print res

        # os.chmod("/home/user/Desktop/Assignment3/output2.csv", 0o777)  # Gives Highest Permission
        # cur.execute("COPY " + OutputTable + " TO '/home/user/Desktop/Assignment3/output2.csv'WITH(FORMAT CSV, HEADER)")

        # cur.close()


def part1(InputTable1, InputTable2, Table1Join, Table2Join, openconnection):
    # connect to postgres
    cur = openconnection.cursor()

    # Gets maximum and min value of column
    cur.execute("SELECT MIN(" + Table1Join + ") FROM " + InputTable1 + "")
    min1 = cur.fetchone()

    cur.execute("SELECT MAX(" + Table1Join + ") FROM " + InputTable1 + "")
    max1 = cur.fetchone()

    cur.execute("SELECT MIN(" + Table2Join + ") FROM " + InputTable2 + "")
    min2 = cur.fetchone()

    cur.execute("SELECT MAX(" + Table2Join + ") FROM " + InputTable2 + "")
    max2 = cur.fetchone()

    min_t1 = (float)(min1[0])
    max_t1 = (float)(max1[0])

    min_t2 = (float)(min2[0])
    max_t2 = (float)(max2[0])

    if max_t1 <= max_t2:
        range_max_val = max_t2
    else:
        range_max_val = max_t1

    if min_t1 <= min_t2:
        range_min_val = min_t1
    else:
        range_min_val = min_t2

    range_max = (range_max_val - range_min_val) / 5

    return range_max, range_min_val

def range_query_insersion(Table1Join, Table2Join, node, openconnection):
    # connect to postgres
    cur = openconnection.cursor()
    cur.execute("INSERT INTO output_table" + str(node) +
                " SELECT * FROM table1_range" + str(node) +
                " INNER JOIN table2_range" + str(node) +
                " ON table1_range" + str(node) +
                "." + Table1Join +
                "=" + "table2_range" + str(node) +
                "." + Table2Join + ";")

    # cur.close()

def range_query(InputTable1, InputTable2, Table1Join, Table2Join, col_name1, col_name2, range_max,range_min_value, openconnection):
    # connect to postgres
    cur = openconnection.cursor()

    for i in range(5):

        if i != 0:
            floor = ceil
            ceil = ceil + range_max
        else:
            floor = range_min_value
            ceil = range_min_value + range_max

        cur.execute("DROP TABLE IF EXISTS table1_range" + str(i) + ";")
        cur.execute("DROP TABLE IF EXISTS table2_range" + str(i) + ";")

        if i != 0:
            cur.execute("CREATE TABLE table1_range" + str(i) +
                        " AS SELECT * FROM " + InputTable1 +
                        " WHERE (" + Table1Join + " <= " + str(ceil) +
                        ") AND (" + Table1Join + " > " + str(floor) + ");")

            cur.execute("CREATE TABLE table2_range" + str(i) +
                        " AS SELECT * FROM " + InputTable2 +
                        " WHERE (" + Table2Join + " <= " + str(ceil) +
                        ") AND (" + Table2Join + " > " + str(floor) + ");")
        else:
            cur.execute("CREATE TABLE table1_range" + str(i) +
                        " AS SELECT * FROM " + InputTable1 +
                        " WHERE (" + Table1Join + " <= " + str(ceil) +
                        ") AND (" + Table1Join + " >= " + str(floor) + ");")

            cur.execute("CREATE TABLE table2_range" + str(i) +
                        " AS SELECT * FROM " + InputTable2 +
                        " WHERE (" + Table2Join + " <= " + str(ceil) +
                        ") AND (" + Table2Join + " >= " + str(floor) + ");")

        # Output Table
        cur.execute("DROP TABLE IF EXISTS output_table" + str(i) + "")
        cur.execute("CREATE TABLE output_table" + str(i) + " (" + col_name1[0][0] + " " + col_name2[0][1] + ")")

        for j in range(1, len(col_name1)):
            cur.execute("ALTER TABLE output_table" + str(i) + " ADD COLUMN " + col_name1[j][0] + " " + col_name1[j][1] + ";")

        for j in range(len(col_name2)):
            cur.execute("ALTER TABLE output_table" + str(i) + " ADD COLUMN " + col_name2[j][0] + "1" + " " + col_name2[j][1] + ";")


################### DO NOT CHANGE ANYTHING BELOW THIS #############################
# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()


# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" % (ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d` + ",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
