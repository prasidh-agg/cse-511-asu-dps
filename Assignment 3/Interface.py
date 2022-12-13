#!/usr/bin/python2.7
#
# Interface for the assignment
#

import psycopg2
from itertools import islice
from StringIO import StringIO


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    cur.execute(
        "CREATE TABLE " + ratingstablename + "(userid INT not null, movieid INT, rating FLOAT, timestamp INT);")
    with open(ratingsfilepath) as newFile:
        for lines in iter(lambda: tuple(islice(newFile, 5000)), ()):
            batch = StringIO()
            batch.write(''.join(line.replace('::', ',') for line in lines))
            batch.seek(0)
            cur.copy_from(batch, ratingstablename, sep=',', columns=('userid', 'movieid', 'rating', 'timestamp'))
    cur.execute("ALTER TABLE " + ratingstablename + " DROP timestamp")
    cur.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    db_name = 'range_part'
    creation_query = "CREATE TABLE IF NOT EXISTS range_meta(part_number INT, from_rat FLOAT, to_rat float)"
    cur.execute(creation_query)

    for i in range(0, numberofpartitions):
        f = i * float(5 / numberofpartitions)
        t = (i + 1) * float(5 / numberofpartitions)
        db_name_part = db_name + str(i)
        create_range = "CREATE TABLE IF NOT EXISTS {db} (UserID INT, movieID INT, Rating FLOAT)".format(db=db_name_part)
        cur.execute(create_range)
        openconnection.commit()

        if i == 0:
            insert_range = "INSERT INTO {db} SELECT * FROM {r} WHERE {r}.rating BETWEEN {f} AND {t}  ".format(
                db=db_name_part,
                r=ratingstablename,
                f=f, t=t)

        else:
            insert_range = "INSERT INTO {db} SELECT * FROM {r} WHERE {r}.rating > {f} AND {t} >= {r}.rating ".format(
                db=db_name_part,
                r=ratingstablename,
                f=f, t=t)

        cur.execute(insert_range)
        openconnection.commit()

        insert_meta = "INSERT INTO range_meta VALUES ({part_number},{f},{t})".format(part_number=i, f=f, t=t)
        cur.execute(insert_meta)

        openconnection.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    db_name = 'rrobin_part'
    create_query = "CREATE TABLE IF NOT EXISTS rrobin_meta(part_number INT, index INT)"
    cur.execute(create_query)
    openconnection.commit()

    sql_temp_create = "CREATE TABLE IF NOT EXISTS rrobin_temp (UserID INT, MovieID INT, Rating FLOAT, idx INT)"
    cur.execute(sql_temp_create)
    openconnection.commit()

    sql_temp_insert = "INSERT INTO rrobin_temp (SELECT {DB}.UserID, {DB}.MovieID, {DB}.Rating , (ROW_NUMBER() OVER() -1) % {n} AS idx FROM {DB})".format(
        n=str(numberofpartitions), DB=ratingstablename)

    cur.execute(sql_temp_insert)
    openconnection.commit()

    for i in range(0, numberofpartitions):
        create_rrobin = "CREATE TABLE IF NOT EXISTS {DB} (UserID INT, MovieID INT, Rating FLOAT)".format(
            DB=db_name + str(i))
        cur.execute(create_rrobin)
        openconnection.commit()
        insert_rrobin = "INSERT INTO {DB} select userid, movieid,rating FROM rrobin_temp WHERE idx = {idx}".format(
            DB=db_name + str(i), idx=str(i))
        cur.execute(insert_rrobin)
        openconnection.commit()

    sql_meta_insert = "INSERT INTO rrobin_meta SELECT {N} AS part_number, count(*) % {N} FROM {DB}".format(
        DB=ratingstablename, N=numberofpartitions)
    cur.execute(sql_meta_insert)
    openconnection.commit()
    deleteTables('rrobin_temp', openconnection)
    openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'rrobin_part%';")

    number_of_partitions = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT (*) FROM " + ratingstablename + ";")
    max_rows = int(cur.fetchone()[0])
    n = max_rows % number_of_partitions
    cur.execute("INSERT INTO rrobin_part" + str(n) + " (UserID,MovieID,Rating) VALUES (" + str(userid) + "," + str(
        itemid) + "," + str(rating) + ");")

    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    selection_query = "SELECT MIN(r.part_number) FROM range_meta AS r WHERE r.from_rat <= {rat} AND r.to_rat >= {rat} ".format(
        rat=rating)
    cur.execute(selection_query)
    openconnection.commit()

    part_number = cur.fetchone()
    p_number = part_number[0]
    rate_insert = "INSERT INTO {db} values ({u},{it},{r})".format(db=ratingstablename, u=userid, it=itemid, r=rating)
    cur.execute(rate_insert)
    openconnection.commit()

    range_insert = "INSERT INTO range_part{i} values ({u},{it},{r})".format(i=p_number, u=userid, it=itemid, r=rating)
    cur.execute(range_insert)
    openconnection.commit()


def createDB(dbname='dds_assignment'):
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
        print ('A database named {0} already exists').format(dbname)

    # Clean up
    cur.close()
    con.close()


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()


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
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
