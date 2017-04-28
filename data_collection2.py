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

MAX_SUMMONERS = 800

INTERMEDIATE_REPORT = 50
INTERMEDIATE_TIME_REPORT = 30
SUMMONER_BATCH_SIZE = 40
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
rawGamesBufferSemaphore = threading._Semaphore(SUMMONER_BATCH_SIZE)
rawGamesLock = thread.allocate_lock()
filteredGames = []
filteredGamesLock = thread.allocate_lock()
filteredGamesBufferSemaphore = threading._Semaphore(SUMMONER_BATCH_SIZE)
flyingGamesSemaphore = threading._Semaphore(2*SUMMONER_BATCH_SIZE)
rawGamesAvailableSemaphore = threading._Semaphore(0)
filteredGamesAvailableSemaphore = threading.Semaphore(0)

rawGamesCollectionActive = True
gamesFilteringActive = True
recordGamesActive = True

duplicate = 0
success = 0
invalid = 0
playersDone = 0
playersInflight = 0
statisticsLock = thread.allocate_lock()


def run(runTime, minRank = 0, maxRank = 36):

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

    INTERMEDIATE_TIME_REPORT = min(runTime/10, 300)

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
    thread.start_new_thread(games_filtering, ())
    thread.start_new_thread(record_games, ())

    reportIndex = 1

    active = True
    lastTimeReport = time.time()


    while active:

        if (time.time() - lastTimeReport) > INTERMEDIATE_TIME_REPORT:

            lastTimeReport = time.time()

            summonersLock.acquire()
            rawGamesLock.acquire()
            filteredGamesLock.acquire()
            recordsLock.acquire()
            statisticsLock.acquire()

            print pf.big_statement("Summoners : " + str(summoners.__len__()) + ", rawGames : " + str(rawGames.__len__())
                                   + ", filteredGames : " + str(filteredGames.__len__())
                                   + " , records : " + str(records.__len__()) + ", in flight : " + str(playersInflight))
            print pf.big_statement(str(success) + " games recorded so far (" + str(duplicate) + " duplicates and " \
                                   + str(invalid) + " invalid games) from " + str(playersDone) + " players in "
                                   + pf.time_format(lastTimeReport))
            reportIndex += 1

            statisticsLock.release()
            recordsLock.release()
            filteredGamesLock.release()
            rawGamesLock.release()
            summonersLock.release()


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
            # if playersDone > reportIndex*INTERMEDIATE_REPORT:
            #
            #     reportIndex += 1

        else :
            #Check for stop condition
            recordsLock.acquire()
            if not recordGamesActive and not records:
                active = False
                recordsLock.release()
            else:
                recordsLock.release()
                time.sleep(1)


            # if n % INTERMEDIATE_REPORT == 0 and n > 0:
            #     print pf.big_statement(
            #     str(success) + " games recorded (" + str(duplicate) + " duplicates and " + str(invalid) \
            #     + " invalid ones) so far with " + str(n) + " players in " + pf.time_format(time.time() - startTime))


    statisticsLock.acquire()

    finalStatement = pf.big_statement(str(success) + " games recorded (" + str(duplicate) + " duplicates and " \
                    + str(invalid) + " invalid games) from " + str(playersDone) + " players in "
                    + pf.time_format(time.time() - startTime) + " using " + str(totalApiCalls) \
                                      + " API calls (" +str(apiErrors) + " errors, 500 : " + str(apiError500) \
            + ", 429 : " + str(apiError429) + ") and sleeping for " + pf.time_format(totalSleepTime) + \
                                      " (" + str(format(totalSleepTime * 100/(time.time() - startTime), '.2f')) + " %)")

    statisticsLock.release()

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
    global rawGamesBufferSemaphore
    global rawGamesCollectionActive
    global filteredGames
    global filteredGamesLock
    global playersInflight
    global statisticsLock

    # lastChance = False
    # stopCondition = False

    while rawGamesCollectionActive:

        # # print summoners
        # if LOG:
        #     log.write(str(summoners[-(SUMMONER_BATCH_SIZE + 5):]) + "\n")


        if ((time.time() - startTime) > runTime):
            rawGamesCollectionActive = False
            rawGamesAvailableSemaphore.release()
        else:

            #The semaphore represent the depth of the rawGames queue
            rawGamesBufferSemaphore.acquire()

            summoner = ""
            summonersLock.acquire()
            if summoners:
                summoner = summoners.pop()
                # print "Adding " + summoner + " to rawGames"
            summonersLock.release()

            if summoner is not "":

                summonersDone.find_insert(summoner)
                statisticsLock.acquire()
                playersInflight += 1
                statisticsLock.release()
                thread.start_new_thread(get_summoner_matchs, (summoner,))
                #Check if time expired

            else:
                #No summoner taken from summoners
                # print "summoners is empty, check for stop condition"
                summonersLock.acquire()
                statisticsLock.acquire()

                if(not summoners and playersInflight == 0):
                    #buffers empty
                    rawGamesCollectionActive = False
                    print "summoners empty and no one in flight"
                    #release semaphore to force next thread to check condition
                    rawGamesAvailableSemaphore.release()

                statisticsLock.release()
                summonersLock.release()

                rawGamesBufferSemaphore.release()
                if rawGamesCollectionActive:
                    time.sleep(1)




    print "Ending raw games collection"

    return ;



def games_filtering():

    global rawGames
    global rawGamesLock
    global recordedGameIds
    global filteredGames
    global filteredGamesLock
    global rawGamesAvailableSemaphore
    global gamesFilteringActive
    global rawGamesCollectionActive
    global playersInflight
    global statisticsLock


    # TODO: we may loose summoners/games in the "buffers", use semaphore


    while gamesFilteringActive:

        # Buffer depth
        filteredGamesBufferSemaphore.acquire()
        
        #TODO: take care of end condition
        rawGamesAvailableSemaphore.acquire()

        rawGamesLock.acquire()


        if rawGames:
            rawGamesBufferSemaphore.release()
            summonerId, data = rawGames.pop(0)
            rawGamesLock.release()

            d = 0
            cleanGames = []

            if "games" in data:
                # check if already recorded
                for game in data["games"]:

                    toRecord = "gameId" in game and not recordedGameIds.find_insert(game["gameId"])

                    if (toRecord):

                        # new game
                        # check RANKED
                        if (game["subType"] == "RANKED_SOLO_5x5"):
                            # add it
                            cleanGames.append(game)

                    else:
                        d += 1

            if cleanGames:

                filteredGamesLock.acquire()
                filteredGames.append((summonerId, cleanGames, d))
                filteredGamesAvailableSemaphore.release()
                filteredGamesLock.release()
                # print "Adding a set of games to filteredGames for summoner " + summonerId
            else:
                # Summoner with no (new or valid) games
                statisticsLock.acquire()
                playersInflight -= 1
                statisticsLock.release()
                filteredGamesBufferSemaphore.release()



        else:
            rawGamesLock.release()
            #give back the spot we took
            filteredGamesBufferSemaphore.release()

            print "RawGames is empty, we should end soon"
            if not rawGamesCollectionActive:
                gamesFilteringActive = False
                filteredGamesAvailableSemaphore.release()
            else:
                print "rawGames empty, we reach this statement(protected by semaphore) and rawGames " \
                      + "collection still active, I am not sure of what is happening"


    print "Ending games filtering"

    return;







    #TODO: clear the buffers

    return ;




def record_games():

    global filteredGames
    global filteredGamesLock
    global flyingGamesSemaphore
    global filteredGamesBufferSemaphore
    global playersInflight
    global statisticsLock
    global recordGamesActive

    while recordGamesActive:

        filteredGamesAvailableSemaphore.acquire()

        gamesToRecord = []

        filteredGamesLock.acquire()
        #At least one is available

        gamesToRecord.extend(filteredGames)
        filteredGames = []
        filteredGamesLock.release()

        filteredGamesAvailableSemaphore.release()

        for i in range(0, gamesToRecord.__len__()):
            filteredGamesBufferSemaphore.release()
            filteredGamesAvailableSemaphore.acquire()

        #logging
        n = 0
        for summonerId, games, d in gamesToRecord:
            n += games.__len__()

        print "Batch of " + str(gamesToRecord.__len__()) + " summoners for a total of " + str(n) + " games"

        if gamesToRecord:

            playersId = []


            for summonerId, games, d in gamesToRecord:

                for game in games :

                    #list all necessary players
                    for player in game["fellowPlayers"]:
                        playersId.append(str(player["summonerId"]))
                    playersId.append(summonerId)

            playersId= list(set(playersId))

            # print "Preparing to retrieve rank for " + str(playersId.__len__()) + " players"

            stats = get_players_rank(playersId)


            for summonerId, games, d in gamesToRecord:

                #Probably does not impact anything, avoid an explosion of the number of threads which is unlikely
                flyingGamesSemaphore.acquire()
                statisticsLock.acquire()
                playersInflight -= 1
                statisticsLock.release()
                thread.start_new_thread(compute_game_records, (games, summonerId, d, stats))

        else:
            if not gamesFilteringActive:
                recordGamesActive = False
            else:
                print "Issue with record games, filtered games is empty"

    print "Ending games recording"

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
    # print("Summoner " + summonerId + " : " + str(s) + " new games, " + str(d) + " duplicate games and " \
    #   + str(i) + " invalid")

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

    print "Starting " + str(n) + " threads to get " + str(summonerIds.__len__()) + " players' rank"

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
        if summonerIds.__len__() > 3:
            stats_cp = sorted(stats.items(), key=operator.itemgetter(1))
            # select the chosen 4 !
            for k in range(2):
                if stats[stats_cp[k][0]] > 0:
                    playersToAppend.append(stats_cp[k][0])
                if stats[stats_cp[len(stats_cp) - 1 - k][0]] > 0:
                    playersToAppend.append(stats_cp[len(stats_cp) - 1 - k][0])

    newSummoners = []
    for summonerId in playersToAppend:
        if not summonersDone.find_insert(summonerId, insert= False):
            newSummoners.append(summonerId)

    if newSummoners:
        summonersLock.acquire()
        summoners.extend(newSummoners)
        random_discard()
        summonersLock.release()


    return ;

def get_summoner_matchs(summonerId):

    global rawGames
    global rawGamesAvailableSemaphore
    global rawGamesLock
    global playersInflight
    global statisticsLock



    requestUrl = matchUrl + summonerId + "/recent"
    apiData = api_call(requestUrl)

    if apiData is not None:
        #Transfer knowledge

        rawGamesLock.acquire()

        rawGames.append((summonerId,apiData))

        rawGamesLock.release()
        rawGamesAvailableSemaphore.release()

    else:
        statisticsLock.acquire()
        playersInflight -= 1
        statisticsLock.release()


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
        print "Error " + str(response.status_code) + " on " + url \
              # + apiKey[currentKey]
        apiErrors += 1
        if(response.status_code == 500):
            apiError500 += 1
            if(tries < 3):
                apiLock.release()
                return api_call(url, tries + 1)
        if(response.status_code == 429):
            apiError429 += 1
            time.sleep(0.2)
            apiLock.release()
            return api_call(url, tries)
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
    global summonersLock

    summonersLock.acquire()

    f = open('summoners.txt', 'r')
    for line in f:
        if line is not "":
            if line[-1:] == "\n":
                summoners.append(line[:-1])
            else:
                summoners.append(line)

    f.close()

    summonersLock.release()


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
