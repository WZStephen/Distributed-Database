#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(tablename, path, con):
    cur = con.cursor()
    #Creating the rating table in dds_assgn1 database
    cur.execute("DROP TABLE IF EXISTS "+tablename)
    cur.execute("CREATE TABLE "+tablename+" (row_id serial primary key,userid INT, temp1 VARCHAR(10),  movieid INT , temp3 VARCHAR(10),  rating REAL, temp5 VARCHAR(10), Timestamp INT)")
    #loading the dataset into the rating table follow as user, movie and rating
    reading = open(path,'r')
    cur.copy_from(reading,tablename,sep = ':',columns=('userid','temp1','movieID','temp3','rating','temp5','Timestamp'))
    cur.execute("ALTER TABLE "+tablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")
    #print "Load ratings success!"
    # Clean up
    cur.close()

def rangepartition(tablename, part_num, openconnection):

    cur = openconnection.cursor()

    global global_range_part_num
    global_range_part_num = part_num

    #define the interval range by number of partitions
    MaxInterval = 5.0 / part_num
    num = 0
    Temp = 0

    #implement the range partition algorithm retrieve data and insert into table by the rating orders
    while Temp <= 4.0:
        if Temp != 0:
            cur.execute("DROP TABLE IF EXISTS range_part" + str(num))
            cur.execute(
                "CREATE TABLE range_part" + str(num) + " AS SELECT * FROM " + tablename + " WHERE Rating>" + str(
                    Temp) + " AND Rating<=" + str(Temp + MaxInterval) + ";")
            Temp = Temp + MaxInterval
            num = num + 1
        else:
            cur.execute("DROP TABLE IF EXISTS range_part" + str(num))
            cur.execute(
                "CREATE TABLE range_part" + str(num) + " AS SELECT * FROM " + tablename + " WHERE Rating>=" + str(
                    Temp) + " AND Rating<=" + str(Temp + MaxInterval) + ";")
            Temp = Temp + MaxInterval
            num = num + 1
    # Clean up
    cur.close()

def roundrobinpartition(tablename, part_num, con):

    cur = con.cursor()
    global global_rr_part_num
    global end

    global_rr_part_num = part_num
    end = 0
    count = 0

    #implement round robin partition insert the data in to rrobin_part table by row name (if row is in 15, then this row will be insert into rrobin table 5)
    for i in list(reversed(range(part_num))):
        cur.execute("DROP TABLE IF EXISTS rrobin_part" + str(i))
        cur.execute("CREATE TABLE rrobin_part" + str(i) + " AS SELECT * FROM " + tablename + " WHERE " + str((i) % part_num) + " = " + "row_id % " + str(part_num))
        rowNo_partition = cur.execute("SELECT COUNT (*) FROM rrobin_part" + str(i))

        if rowNo_partition > count:
            end = i
            count = rowNo_partition
    # Clean up
    cur.close()

def roundrobininsert(tablename, userid, itemid, rating, con):

    cur = con.cursor()
    global end
    global global_rr_part_num

    rr_part_num = end % global_rr_part_num

    #impleemnt round robin intesert algorithm to insert the data into proper rrobin partition table
    cur.execute("INSERT INTO rrobin_part" + str(rr_part_num) + " (UserID,MovieID,Rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ")")
    # Clean up
    cur.close()


def rangeinsert(tablename, userid, itemid, rating, con):
    cur = con.cursor()

    global global_range_part_num
    MaxInterval_ = 5.0 / global_range_part_num

    floor = 0
    ceil = MaxInterval_
    num_ = 0

    while floor < 5.0:
        if floor != 0:
            if rating > floor and rating <= ceil:
                break
            else:
                num_ = num_ + 1
                floor = floor + MaxInterval_
                ceil = ceil + MaxInterval_
        else:
            if rating > floor and rating <= ceil:
                break
            else:
                num_ = num_ + 1
                floor = floor + MaxInterval_
                ceil = ceil + MaxInterval_
    #implement rage insert algorithm to find which ranage partition table should be inserted , then insert the data into the table by rating interval
    cur.execute("INSERT INTO range_part" + str(num_) + " (UserID,MovieID,Rating) VALUES (%s, %s, %s)",(userid, itemid, rating))
    # Clean up
    cur.close()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()

""""
if __name__=='__main__':
    try:
        create_db(DATABASE_NAME)
        with getopenconnection() as con:
            roundrobinpartition('ratings', 5, con=con)
            roundrobininsert('ratings', userid = 2, itemid = 4, rating = 6, con = con)
            cur = con.cursor()
            cur.execute("SELECT * FROM rrobin_part5 where row_id=15")
            res = cur.fetchall()
            print res
            cur.close()


    except Exception as E:
        print "Error is ", E
"""



	


	
	




