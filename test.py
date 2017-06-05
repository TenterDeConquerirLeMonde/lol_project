import sqlite3
import requests
import json
import time
import random
import thread
import threading
import traceback
import sys
import Queue
import structures as struct
import read_db as rdb

def test():

    conn = sqlite3.connect("lol-euw.db")
    c = conn.cursor()

    for i in range(1,100):
        x = 3100000000 + i * 1000000

        c.execute("SELECT COUNT(gameId) FROM matchs WHERE gameId > " + str(x))
        print "x > " + str(x) + " : " + str(c.fetchone()[0])

    conn.close()
    return


def deleteEntries():

    regions = ['euw', 'na']
    limits = [3120000000, 2450000000]

    for i in range(0,2):

        conn = sqlite3.connect("lol-" + regions[i] + ".db")
        c = conn.cursor()

        c.execute("DELETE FROM matchs WHERE gameId < " + str(limits[i]))

        conn.commit()

        conn.close()



rdb.merge_dbs([('euw', 3185117746, 0)], 'test')