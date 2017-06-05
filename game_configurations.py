import sqlite3
import requests
import json
import time
import math
import random
import thread
import threading
import traceback
import sys
import Queue
import structures as struct
import read_db as rdb
import print_functions as pf


class Team(object):

    def __init__(self, champions):

        fp = 0
        for c in champions:
            fp = 1000*fp + c
        self.fingerprint = fp
        self.n = 1

    def equals(self, otherTeam):

        if self.fingerprint == otherTeam.fingerprint:
            #All equals
            self.n += otherTeam.n
            return True
        else:
            return False

def teamConfigurationChunk(region, condition, id, answerQueue):
    #go through these games and add answer to the queue (merge sort like)
    BATCH = 800
    n = 0
    teams = []
    rawTeams = []
    start = time.time()

    conn = sqlite3.connect('lol-' + region + '.db')
    c = conn.cursor()

    games = c.execute("SELECT * FROM matchs" + condition)


    for m in games:

        if n % 25000 == 0 and n != 0:
            print str(id) + " " + str(teams.__len__() + rawTeams.__len__()) + " team configurations over " + str(n) \
                  + " games"

        n += 1

        t1 = Team(sorted(m[1:6]))
        t2 = Team(sorted(m[6:11]))

        if not checkKnownTeams(teams, rawTeams, t1):
            rawTeams.append(t1)
        if not checkKnownTeams(teams, rawTeams, t2):
            rawTeams.append(t2)

        if rawTeams.__len__() > BATCH:
            teams.extend(rawTeams)
            rawTeams = []
            teams.sort(key=lambda x: x.fingerprint)

    teams.extend(rawTeams)

    # teams.sort(key=lambda x: x.n)
    # print list(x.n for x in teams[-50:])

    print region.upper() + " " + str(id) + " done, it contains " + str(teams.__len__()) + " team configurations in " + str(n) \
          + " games, done in " + str(format(time.time() - start, '.1f')) + " s"
    # teams.sort(key=lambda  x : x.fingerprint)

    answerQueue.put(teams)



def teamConfiguration(regions):

    start = time.time()

    configQueue = Queue.Queue()
    THREADS = 10
    mergeLock = thread.allocate_lock()
    mergeLock.acquire()

    thread.start_new_thread(mergeConfigs, (configQueue, mergeLock, THREADS * regions.__len__()))

    total = 0


    for region in regions:

        conn = sqlite3.connect('lol-' + region + '.db')
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM matchs")

        total += c.fetchone()[0]

        conn.close()


        limits = rdb.gameIdsListing(100/THREADS, region)
        limits[0] = 0
        limits[THREADS] = 2* limits[THREADS]

        for i in range(0, THREADS):

            condition = " WHERE gameId >= " + str(limits[i]) + " and gameId < " + str(limits[i + 1])
            thread.start_new_thread(teamConfigurationChunk, (region, condition, i + 1 , configQueue))

            print "Starting thread " + region.upper() + " " + str(i + 1) + " with condition " + condition




    mergeLock.acquire()

    fullConfig = configQueue.get()
    print pf.big_statement("Full config for " + str(regions) + " computed in " + pf.time_format(time.time() - start))

    print pf.big_statement(fullConfig.__len__() + " team configuration in " + str(total) + " games")

    fullConfig.sort(key=lambda x : x.n)

    print list(x.n for x in fullConfig[-100:])

def teamConfigurationByBlocks(regions):

    start = time.time()

    configQueue = Queue.Queue()
    mergeLock = thread.allocate_lock()
    mergeLock.acquire()

    BLOCK_SIZE = 50000
    total = 0
    blocks = 0

    for region in regions:

        conn = sqlite3.connect('lol-' + region + '.db')
        c = conn.cursor()

        c.execute("SELECT COUNT(*) FROM matchs")
        totalRegion = c.fetchone()[0]
        total += totalRegion

        blocks += int(math.ceil(float(totalRegion) / BLOCK_SIZE))

        conn.close()

    thread.start_new_thread(mergeConfigs, (configQueue, mergeLock, blocks))

    start = time.time()

    for region in regions:


        BATCH = 800
        n = 0
        teams = []
        rawTeams = []
        startRegion = time.time()

        conn = sqlite3.connect('lol-' + region + '.db')
        c = conn.cursor()

        games = c.execute("SELECT * FROM matchs")

        for m in games:

            if n % 10000 == 0 and n != 0:
                print str(teams.__len__() + rawTeams.__len__()) + " team configurations over " + str(n) \
                      + " games"

            n += 1

            t1 = Team(sorted(m[1:6]))
            t2 = Team(sorted(m[6:11]))

            if not checkKnownTeams(teams, rawTeams, t1):
                rawTeams.append(t1)
            if not checkKnownTeams(teams, rawTeams, t2):
                rawTeams.append(t2)

            if rawTeams.__len__() > BATCH:
                teams.extend(rawTeams)
                rawTeams = []
                teams.sort(key=lambda x: x.fingerprint)

            if n % BLOCK_SIZE == 0 and n != 0:
                teams.extend(rawTeams)
                configQueue.put(teams)
                print("Adding a block of team configuration")
                rawTeams = []
                teams = []

        teams.extend(rawTeams)
        configQueue.put(teams)
        print("Adding a block of team configuration")

        # teams.sort(key=lambda x: x.n)
        # print list(x.n for x in teams[-50:])

        print region.upper() + " done, it contains " + str(
            teams.__len__()) + " team configurations in " + str(n) \
              + " games, done in " + str(format(time.time() - startRegion, '.1f')) + " s"
        # teams.sort(key=lambda  x : x.fingerprint)

        conn.close()


    mergeLock.acquire()

    fullConfig = configQueue.get()
    print pf.big_statement("Full config for " + str(regions) + " computed in " + pf.time_format(time.time() - start))

    print pf.big_statement(str(fullConfig.__len__()) + " team configuration in " + str(total) + " games")

    fullConfig.sort(key=lambda x : x.n)

    print list(x.n for x in fullConfig[-100:])

def mergeConfigs(configQueue, lock, n):

    masterConfig = configQueue.get()
    print "Retrieving seed master config"

    for _ in range(1, n):

        config = configQueue.get()
        print "Retrieving new config"

        config.sort(key=lambda  x : x.fingerprint)
        masterConfig.sort(key=lambda  x : x.fingerprint)

        newTeams = []

        for t in config:

            if not checkKnownTeams(masterConfig, [], t):
                newTeams.append(t)

        masterConfig.extend(newTeams)
        # masterConfig.sort(key=lambda  x : x.n)

        print "\nMerged with master config, we now have " + str(masterConfig.__len__()) + " team configuration\n"
        # print list(x.n for x in masterConfig[-50:])


    print "Done merging configs"

    configQueue.put(masterConfig)

    lock.release()


def binarySearch(alist, item):
    first = 0
    last = len(alist)-1
    found = False

    while first <= last and not found:
        midpoint = (first + last)//2
        if alist[midpoint].equals(item):
            found = True
        else:
            if item.fingerprint < alist[midpoint].fingerprint:
                last = midpoint-1
            else:
                first = midpoint+1

    return found

def checkKnownTeams(teams, rawTeams, t):

    i = 0
    b = binarySearch(teams, t)

    while not b and i < rawTeams.__len__():
        b = rawTeams[i].equals(t)
        i+= 1


    return b


if __name__ == "__main__":
    teamConfigurationByBlocks(sys.argv[1:])



