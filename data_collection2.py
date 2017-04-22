import sqlite3
import requests
import json
import time
import random
import thread
import threading
import operator
import sys
import structures as struct

import print_functions as pf
import read_db as rdb

MAX_API_CALLS = 10
WAIT_TIME_SECONDS = 12.2

RUN_TIME = 60

MAX_SUMMONERS = 500

INTERMEDIATE_REPORT = 25
SUMMONER_BATCH_SIZE = 20
LOG = False

matchUrl = "https://na.api.pvp.net/api/lol/na/v1.3/game/by-summoner/"
rankUrl = "https://na.api.pvp.net/api/lol/na/v2.5/league/by-summoner/"

testSummonerId = "3675"

#testApiKey = "?api_key=" + "RGAPI-B34D1399-0E8A-4DF4-86A2-FF6B9264E79A"

apiKey = []
firstCallTime = []
apiCalls = 0
currentKey = 0

totalApiCalls = 0
totalSleepTime = 0
apiError500 = 0
apiError429 = 0
apiErrors = 0

apiLock = thread.allocate_lock()
recordsLock = thread.allocate_lock()
records = []

summoners = []
summonersLock = thread.allocate_lock()
summonersDone = None
recordedGameIds = None

minRankLimit = 0
maxRankLimit = 36
targeted = False

rawGames = []
rawGamesSemaphore = threading._Semaphore(SUMMONER_BATCH_SIZE)
rawGamesLock = thread.allocate_lock()
filteredGames = []
filteredGamesLock = thread.allocate_lock()
flyingGamesSemaphore = threading._Semaphore(SUMMONER_BATCH_SIZE)

duplicate = 0

success = 0
invalid = 0
playersDone = 0
statisticsLock = thread.allocate_lock()


def run(runTime = RUN_TIME, minRank = 0, maxRank = 36):

    global summoners
    global summonersDone
    global recordedGameIds
    global minRankLimit
    global maxRankLimit
    global targeted
    global records
    global recordsLock

    if minRank is not minRankLimit or maxRank is not maxRankLimit:
        targeted = True
        print "Targeted search " + str(minRank) + " to " + str(maxRank) + "\n"

    minRankLimit = minRank
    maxRankLimit = maxRank

    random.seed()

    startTime = time.time()

    conn = sqlite3.connect('lol.db')
    c = conn.cursor()

    load_keys()
    load_summoners()

    #cheating :
    summonersDone = struct.Locked_BST(["5"])

    gameIds = list(c.execute("SELECT gameId FROM matchs"))
    recordedGameIds = struct.Locked_BST(gameIds)



    if LOG:
        log = open('log.txt', 'w')


        # print summoners
    if LOG:
        log.write(str(summoners[-(SUMMONER_BATCH_SIZE + 5):]) + "\n")

    # summoner = summoners.pop()

    thread.start_new_thread(raw_games_collection, (startTime, runTime))
    thread.start_new_thread(games_filtering, (startTime, runTime))
    thread.start_new_thread(record_games, (startTime, runTime))

    while (time.time() - startTime) < runTime:

        newRecords = []

        recordsLock.acquire()
        newRecords.extend(records)
        records = []
        recordsLock.release()


        if newRecords :

            for r in newRecords:

                sqlAction = "INSERT INTO matchs VALUES(" + ','.join(map(str, r)) + ")"
                # print sqlAction
                c.execute(sqlAction)

            conn.commit()

        else :
            time.sleep(1)


            # if n % INTERMEDIATE_REPORT == 0 and n > 0:
            #     print pf.big_statement(
            #     str(success) + " games recorded (" + str(duplicate) + " duplicates and " + str(invalid) \
            #     + " invalid ones) so far with " + str(n) + " players in " + pf.time_format(time.time() - startTime))



    finalStatement = pf.big_statement(str(success) + " games recorded (" + str(duplicate) + " duplicates and " \
                    + str(invalid) + " invalid games) from " + str(playersDone) + " players in "
                    + pf.time_format(time.time() - startTime) + " using " + str(totalApiCalls) \
                                      + " API calls (" +str(apiErrors) + " errors, 500 : " + str(apiError500) \
            + ", 429 : " + str(apiError429) + ") and sleeping for " + pf.time_format(totalSleepTime) + \
                                      " (" + str(format(totalSleepTime * 100/runTime, '.2f')) + " %)")


    f = open('summoners.txt', 'w')
    f.writelines("\n".join(summoners))
    f.close()

    print finalStatement

    print "storing " + str(summoners.__len__()) + " summonerIds for next time"

    c.execute("SELECT COUNT(*) FROM matchs")
    totaldb = c.fetchone()[0]

    conn.close()
    if LOG:
        log.close()


    print pf.big_statement("The database contains " + str(totaldb) + " games")


    # finalStatement += rdb.average_rank(0.5)

    # report(totaldb, successList, invalidList, duplicateList, finalStatement)


    return ;

def raw_games_collection(startTime, runTime):
    global rawGames
    global rawGamesLock
    global summoners
    global summonersLock
    global summonersDone
    global rawGamesSemaphore

    while (time.time() - startTime) < runTime:

        # # print summoners
        # if LOG:
        #     log.write(str(summoners[-(SUMMONER_BATCH_SIZE + 5):]) + "\n")

        #The semaphore represent the depth of the rawGames queue
        rawGamesSemaphore.acquire()

        summoner = ""
        summonersLock.acquire()
        if summoners.__len__() > 0:
            summoner = summoners.pop()
            # print "Adding " + summoner + " to rawGames"
        summonersLock.release()

        if summoner is not "":
            summonersDone.find_insert(summoner)
            thread.start_new_thread(get_summoner_matchs, (summoner,))
        else:
            #No summoner taken from summoners
            rawGamesSemaphore.release()
            time.sleep(1)





def games_filtering(startTime, runTime):

    global rawGames
    global rawGamesLock
    global recordedGameIds
    global filteredGames
    global filteredGamesLock


    # TODO: we may loose summoners/games in the "buffers", use semaphore


    while (time.time() - startTime) < runTime:

        gamesToFilter = []


        rawGamesLock.acquire()
        filteredGamesLock.acquire()

        n = SUMMONER_BATCH_SIZE - filteredGames.__len__()

        if rawGames.__len__() > 0 and n > 0:

            m = min(rawGames.__len__(), n)

            for i in range(0, m):
                gamesToFilter.append(rawGames.pop(0))

        filteredGamesLock.release()
        rawGamesLock.release()

        print "filteredGames contains " + str(SUMMONER_BATCH_SIZE - n) + " elements, preparing " + str(gamesToFilter.__len__()) + " new ones"



        if (not gamesToFilter):
            ##Empty so filteredGames is full or raw games is empty

            time.sleep(1)

        else:

            d = 0

            for summonerId, data in gamesToFilter:

                cleanGames = []

                if "games" in data:
                    # check if already recorded
                    for game in data["games"]:

                        alreadyRecorded = "gameId" in game and recordedGameIds.find_insert(game["gameId"])


                        if (not alreadyRecorded):

                            # new game
                            # check RANKED
                            if (game["subType"] == "RANKED_SOLO_5x5"):
                                #add it
                                cleanGames.append(game)

                        else :
                            d += 1

                if cleanGames:

                    filteredGamesLock.acquire()
                    filteredGames.append((summonerId, cleanGames, d))
                    filteredGamesLock.release()
                else :
                    #Summoner with no (new) games
                    print("Summoner " + summonerId + " : " + str(d) + " duplicate games ")




    #TODO: clear the buffers

    return ;




def record_games(startTime, runTime):

    global filteredGames
    global filteredGamesLock
    global flyingGamesSemaphore

    while (time.time() - startTime) < runTime:



        gamesToRecord = []

        filteredGamesLock.acquire()

        gamesToRecord.extend(filteredGames)
        filteredGames = []

        filteredGamesLock.release()

        print str(gamesToRecord.__len__()) + " games to record for this batch"

        if gamesToRecord:

            playersId = []


            for summonerId, games, d in gamesToRecord:

                for game in games :

                    #list all necessary players
                    for player in game["fellowPlayers"]:
                        playersId.append(str(player["summonerId"]))
                    playersId.append(summonerId)

            playersId= list(set(playersId))

            print "Preparing to retrieve rank for " + str(playersId.__len__()) + " players"

            stats = get_players_rank(playersId)


            for summonerId, games, d in gamesToRecord:

                #Probably does not impact anything, avoid an explosion of the number of threads which is unlikely
                flyingGamesSemaphore.acquire()
                thread.start_new_thread(compute_game_records, (games, summonerId, d, stats))

        else:
            time.sleep(1)


    return ;



def compute_game_records(games, summonerId, d, stats):

    global playersDone
    global success
    global invalid
    global duplicate
    global statisticsLock
    global recordsLock
    global records
    global flyingGamesSemaphore

    localRecords = []

    s = 0
    i = 0

    for game in games :


        record = []

        champsTeam1 = []
        champsTeam2 = []
        levelTeam1 = []
        levelTeam2 = []


        winnerTeam = (2 - game["stats"]["win"]) * game["teamId"] % 300

        if (game["teamId"] == 100):
            champsTeam1.append(game["championId"])
            levelTeam1.append(stats[summonerId])

        else:
            champsTeam2.append(game["championId"])
            levelTeam2.append(stats[summonerId])

        for player in game["fellowPlayers"]:
            if (player["teamId"] == 100):
                champsTeam1.append(player["championId"])
                levelTeam1.append(stats[str(player["summonerId"])])
            else:
                champsTeam2.append(player["championId"])
                levelTeam2.append(stats[str(player["summonerId"])])

        record.append(game["gameId"])
        record.extend(champsTeam1)
        record.extend(champsTeam2)
        record.extend(levelTeam1)
        record.extend(levelTeam2)
        record.append(winnerTeam)

        if record.__contains__(-1):
            i += 1
        else:
            localRecords.append(record)
            s += 1


    recordsLock.acquire()

    records.extend(localRecords)

    recordsLock.release()

    # print(str(i - 1) + " games added to db")
    print("Summoner " + summonerId + " : " + str(s) + " new games, " + str(d) + " duplicate games and " \
      + str(i) + " invalid")

    statisticsLock.acquire()
    success += s
    invalid += i
    duplicate += d
    playersDone += 1
    statisticsLock.release()

    flyingGamesSemaphore.release()


    return ;

def get_players_rank(summonerIds):
    #split in block of 10, fan out to multiple thread
    #Rearrange all of it

    locks = []
    stats = []

    n = summonerIds.__len__()/10 + 1
    if summonerIds.__len__() % 10 == 0:
        n -= 1

    print " Starting " + str(n) + " threads to get " + str(summonerIds.__len__()) + " players' rank"

    for i in range(0, n):
        newLock = thread.allocate_lock()
        locks.append(newLock)
        newLock.acquire()
        newStatsChunck = {}
        stats.append(newStatsChunck)

        if i == (n - 1):
            thread.start_new_thread(bulk_rank_stats, (summonerIds[10*i:summonerIds.__len__()], newLock, newStatsChunck))
        else:
            thread.start_new_thread(bulk_rank_stats, (summonerIds[10*i:10*(i+1)], newLock, newStatsChunck))


    for lock in locks:
        lock.acquire()

    fullStats = {}
    for s in stats:
        fullStats.update(s)

    return fullStats




def bulk_rank_stats(summonerIds, lock, stats):
    #initialize ranks

    # print "Bulk rank stats for " + str(summonerIds.__len__()) + " players"

    for id in summonerIds:
        stats[id] = -1

    requestUrl = rankUrl + ','.join(map(str, summonerIds)) + "/entry"

    data = api_call(requestUrl)

    if(data is not None):
    #	print("rank response ok")
        for id in summonerIds:

            if id in data.keys() :
                for entry in data[id] :
                    if(entry["queue"] == "RANKED_SOLO_5x5"):
                        for x in entry["entries"]:
                            stats[id] = rank_conversion(entry["tier"], x["division"])


    else:
        print "Rank failure"


    lock.release()

    playersToAppend = []

    if targeted:
        # If targeted take all
        for p in summonerIds:
            if stats[p] >= minRankLimit and stats[p] <= maxRankLimit:
                playersToAppend.append(p)


    else:
        # Not targeted take top 2 and bottom 2
        # sort players by rank asc
        stats_cp = sorted(stats.items(), key=operator.itemgetter(1))
        # select the chosen 4 !
        for k in range(2):
            playersToAppend.append(stats_cp[k][0])
            playersToAppend.append(stats_cp[len(stats_cp) - 1 - k][0])

    newSummoners = []
    for summonerId in playersToAppend:
        if not summonersDone.find_insert(summonerId, insert= False):
            newSummoners.append(summonerId)

    summonersLock.acquire()
    summoners.extend(newSummoners)
    random_discard()
    summonersLock.release()


    return ;

def get_summoner_matchs(summonerId):

    global rawGames
    global rawGamesSemaphore
    global rawGamesLock

    requestUrl = matchUrl + summonerId + "/recent"
    apiData = api_call(requestUrl)

    if apiData is not None:
        #Transfer knowledge

        rawGamesLock.acquire()
        rawGames.append((summonerId,apiData))
        rawGamesLock.release()
        rawGamesSemaphore.release()



    return ;


def api_call(url, tries = 0) :

    global apiLock
    global apiCalls
    global firstCallTime
    global totalApiCalls
    global totalSleepTime
    global currentKey
    global apiError429
    global apiError500
    global apiErrors

    apiLock.acquire()


    if(apiCalls == MAX_API_CALLS):
        #Reach max on this key go to next key
        currentKey = (currentKey + 1) % apiKey.__len__()


        apiCalls = 0

    if(apiCalls == 0) :
        #Check if need to wait
        sleepTime = firstCallTime[currentKey]- time.time() + WAIT_TIME_SECONDS
        if(sleepTime > 0):
            #Need to sleep
            totalSleepTime += sleepTime
            time.sleep(sleepTime)

        firstCallTime[currentKey] = time.time()

    apiCalls += 1
    totalApiCalls += 1

    apiLock.release()

    response = requests.get(url + apiKey[currentKey])


    if(not response.ok) :
        apiLock.acquire()
        print "Error " + str(response.status_code) + " on " + url + apiKey[currentKey]
        apiErrors += 1
        if(response.status_code == 500):
            apiError500 += 1
            if(tries < 3):
                apiLock.release()
                return api_call(url, tries + 1)
        if(response.status_code == 429):
            apiError429 += 1
        apiLock.release()
        return None;

    return json.loads(response.content)

def load_keys():

    global firstCallTime
    global apiKey

    f = open('keys.txt', 'r')
    for line in f:
        apiKey.append("?api_key=" + line[:42])
        firstCallTime.append(0)

    f.close()

    return ;


def load_summoners():

    global summoners
    f = open('summoners.txt', 'r')
    for line in f:
        if line is not "":
            if line[-1:] == "\n":
                summoners.append(line[:-1])
            else:
                summoners.append(line)

    f.close()


def report(n, success, invalid, duplicate, finalStatement):
    f = open('report-' + str(n) + '.txt', 'w')
    report = ["Reporting every " + str(INTERMEDIATE_REPORT) + " players\n\n"]

    players = 0
    s = 0
    inv =0
    d = 0

    for i in range(0, success.__len__()):

        players += 1
        s += success[i]
        d += duplicate[i]
        inv += invalid[i]

        if(players == INTERMEDIATE_REPORT):
            report.append(str(s) + " games, " + str(d) + " duplicates and " \
                      + str(inv) + " invalids")
            players = 0
            s = 0
            inv = 0
            d = 0

    report.append(str(s) + " games, " + str(d) + " duplicates and " \
                  + str(inv) + " invalids (" + str(players) + " players)")

    f.writelines("\n".join(report))

    f.write(finalStatement)
    f.write(pf.big_statement("Total games in database : " + str(n)))

    f.close()

    return ;

def rank_conversion(tier, division):

    rank = -10
    if(tier == "BRONZE"):
        rank = 0
    if (tier == "SILVER"):
        rank = 5
    if (tier == "GOLD"):
        rank = 10
    if (tier == "PLATINUM"):
        rank = 15
    if (tier == "DIAMOND"):
        rank = 20
    if (tier == "MASTER"):
        rank = 25
    if (tier == "CHALLENGER"):
        rank = 30



    rank = rank + division_conversion(division)

    return rank;

def division_conversion(division):
    if(division == "V"):
        return 1
    if (division == "IV"):
        return 2
    if (division == "III"):
        return 3
    if (division == "II"):
        return 4
    if (division == "I"):
        return 5


def random_discard():
    global summoners
    #remove duplicates
    summoners = list(set(summoners))

    toDiscard = summoners.__len__() - MAX_SUMMONERS

    # print("Discarding " + str(toDiscard) + " players")

    if toDiscard > 0:


        # f = open('summoners.txt', 'r+')

        for i in range(toDiscard):
            toRemove = random.randint(0, summoners.__len__() - 1)
            # if random.randint(0, 99) < 20 :
            #     f.write(summoners[toRemove] + "\n")

            summoners.remove(summoners[toRemove])

        random.shuffle(summoners)

    # print summoners

    return;




if __name__ == "__main__":
    # print sys.argv
    if len(sys.argv) == 2:
        run(runTime=int(sys.argv[1]))

    elif len(sys.argv) == 4:
        run(runTime= int(sys.argv[1]), minRank= int(sys.argv[2]), maxRank= int(sys.argv[3]))

    else :
        print "problem with arguments"
