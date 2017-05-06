import sqlite3
import requests
import json
import time
import random
import thread
import threading
import operator
import Queue
import structures as struct

import print_functions as pf
import read_db as rdb

MAX_API_CALLS = 10
WAIT_TIME_SECONDS = 12.2

MAX_SUMMONERS = 800

SUMMONER_BATCH_SIZE = 40
LOG = False






class RegionCollection(object):


    def __init__(self, region, runTime, minRank = 0, maxRank = 36):

        if region == "na":
            self.matchUrl = "https://na.api.pvp.net/api/lol/na/v1.3/game/by-summoner/"
            self.rankUrl = "https://na.api.pvp.net/api/lol/na/v2.5/league/by-summoner/"
        elif region == "euw":
            self.matchUrl = "https://euw.api.pvp.net/api/lol/euw/v1.3/game/by-summoner/"
            self.rankUrl = "https://euw.api.pvp.net/api/lol/euw/v2.5/league/by-summoner/"
        else:
            return

        self.runTime = runTime
        self.region = region
        self.minRankLimit = minRank
        self.maxRankLimit = maxRank

        if minRank is not 0 or maxRank is not 36:
            self.targeted = True
        else:
            self.targeted = False


        #API parameters
        self.apiKey = []
        self.firstCallTime = []
        self.currentKey = 0
        self.apiCalls = 0

        self.totalApiCalls = 0
        self.totalSleepTime = 0
        self.apiError500 = 0
        self.apiError429 = 0
        self.apiErrors = 0

        self.apiLock = thread.allocate_lock()

        #Binary Search Trees
        self.summonersDone = None
        self.recordedGameIds = None
        self.invalidSummoners = None

        #Rank parameters


        #Lists and queues for sequential treatment

        self.summoners = []
        self.summonersLock = thread.allocate_lock()

        self.summonersToBeTreated = Queue.Queue(1.5*SUMMONER_BATCH_SIZE)

        self.rawGames = Queue.Queue(1.5*SUMMONER_BATCH_SIZE)

        self.filteredGames = Queue.Queue(SUMMONER_BATCH_SIZE)

        self.recordsLock = thread.allocate_lock()
        self.records = []

        self.flyingGamesSemaphore = threading._Semaphore(2*SUMMONER_BATCH_SIZE)

        #Threads stop condition

        self.rawGamesCollectionActive = True
        self.gamesFilteringActive = True
        self.recordGamesActive = True
        self.rawGamesDone = False

        #Statistics

        self.statisticsLock = thread.allocate_lock()

        self.duplicate = 0
        self.success = 0
        self.invalid = 0
        self.playersDone = 0
        self.playersInflight = 0

        self.dataGathering = thread.allocate_lock()
        self.dataGathering.acquire()

    def run(self):
        thread.start_new_thread(self.run_region, ())

    def wait_for_end(self):
        self.dataGathering.acquire()

    def run_region(self):

        if self.targeted:
            print "Targeted search on " + self.region.upper() + " : " + str(self.minRankLimit) + " to " + str(self.maxRankLimit) + "\n"
        else:
            print "Untargeted search on " + self.region.upper() + "\n"


        INTERMEDIATE_TIME_REPORT = min(self.runTime/5, 300)

        random.seed()

        startTime = time.time()

        conn = sqlite3.connect("lol-" + self.region + ".db")
        c = conn.cursor()

        self.load_keys()
        self.load_summoners()

        #cheating :
        self.summonersDone = struct.Locked_BST(["5"])

        gameIds = list(c.execute("SELECT gameId FROM matchs"))
        if gameIds:
            self.recordedGameIds = struct.Locked_BST(gameIds)
        else :
            self.recordedGameIds = struct.Locked_BST([5])


        if LOG:
            log = open('log' + self.region+ '.txt', 'w')


        if LOG:
            log.write(str(self.summoners[-(SUMMONER_BATCH_SIZE + 5):]) + "\n")

        thread.start_new_thread(self.raw_games_collection, (startTime,))
        thread.start_new_thread(self.games_filtering, ())
        thread.start_new_thread(self.record_games, ())


        active = True
        lastTimeReport = time.time()


        while active:

            if (time.time() - lastTimeReport) > INTERMEDIATE_TIME_REPORT:

                lastTimeReport = time.time()

                self.summonersLock.acquire()
                self.recordsLock.acquire()
                self.statisticsLock.acquire()

                print pf.big_statement(self.region.upper() + " Summoners : " + str(self.summoners.__len__()) + ", toBeTreated : " +
                                       str(self.summonersToBeTreated.qsize()) + ", rawGames : " + str(self.rawGames.qsize())
                                       + ", filteredGames : " + str(self.filteredGames.qsize())
                                       + " , records : " + str(self.records.__len__()) + ", in flight : " + str(self.playersInflight))
                print pf.big_statement(self.region.upper() + " " + str(self.success) + " games recorded so far (" + str(self.duplicate) + " duplicates and " \
                                       + str(self.invalid) + " invalid games) from " + str(self.playersDone) + " players in "
                                       + pf.time_format(lastTimeReport - startTime))

                self.statisticsLock.release()
                self.recordsLock.release()
                self.summonersLock.release()


            newRecords = []

            self.recordsLock.acquire()
            newRecords.extend(self.records)
            self.records = []
            self.recordsLock.release()


            if newRecords :

                for r in newRecords:

                    sqlAction = "INSERT INTO matchs VALUES(" + ','.join(map(str, r)) + ")"
                    c.execute(sqlAction)

                conn.commit()


            else :
                #Check for stop condition
                self.recordsLock.acquire()
                if not self.recordGamesActive and not self.records:
                    active = False
                    self.recordsLock.release()
                else:
                    self.recordsLock.release()
                    time.sleep(1)


        self.statisticsLock.acquire()

        print pf.big_statement( "All done : " +
            self.region.upper() + " Summoners : " + str(self.summoners.__len__()) + ", toBeTreated : " +
            str(self.summonersToBeTreated.qsize()) + ", rawGames : " + str(self.rawGames.qsize())
            + ", filteredGames : " + str(self.filteredGames.qsize())
            + " , records : " + str(self.records.__len__()) + ", in flight : " + str(self.playersInflight))

        finalStatement = pf.big_statement(str(self.success) + " games recorded (" + str(self.duplicate) + " duplicates and " \
                        + str(self.invalid) + " invalid games) from " + str(self.playersDone) + " players in "
                        + pf.time_format(time.time() - startTime) + " using " + str(self.totalApiCalls) \
                                          + " API calls (" +str(self.apiErrors) + " errors, 500 : " + str(self.apiError500) \
                + ", 429 : " + str(self.apiError429) + ") and sleeping for " + pf.time_format(self.totalSleepTime) + \
                                          " (" + str(format(self.totalSleepTime * 100/(time.time() - startTime), '.2f')) + " %)")


        self.statisticsLock.release()

        f = open('summoners-' + self.region + '.txt', 'w')
        f.writelines("\n".join(self.summoners))
        f.close()

        print finalStatement

        print self.region.upper() + " storing " + str(self.summoners.__len__()) + " summonerIds for next time"

        c.execute("SELECT COUNT(*) FROM matchs")
        totaldb = c.fetchone()[0]

        conn.close()
        if LOG:
            log.close()


        print pf.big_statement("The " + self.region.upper() + " database contains " + str(totaldb) + " games")


        # finalStatement += rdb.average_rank(0.5)

        # report(totaldb, successList, invalidList, duplicateList, finalStatement)

        self.dataGathering.release()

        return ;

    def raw_games_collection(self, startTime):


        #Starting the thread pool

        threadsLocks = []


        for i in range(0, 2*SUMMONER_BATCH_SIZE):
            lock = thread.allocate_lock()
            lock.acquire()
            threadsLocks.append(lock)
            thread.start_new_thread(self.get_summoner_matchs, (lock, i))

        while self.rawGamesCollectionActive:


            if ((time.time() - startTime) > self.runTime):
                self.rawGamesCollectionActive = False
            else:


                summoner = ""
                self.summonersLock.acquire()
                if self.summoners:
                    summoner = self.summoners.pop()
                self.summonersLock.release()

                if summoner is not "":

                    self.summonersDone.find_insert(summoner)
                    self.statisticsLock.acquire()
                    self.playersInflight += 1
                    self.statisticsLock.release()
                    self.summonersToBeTreated.put(summoner)

                else:
                    #No summoner taken from summoners
                    self.summonersLock.acquire()
                    self.statisticsLock.acquire()

                    if(not self.summoners and self.playersInflight == 0):
                        #buffers empty
                        self.rawGamesCollectionActive = False
                        print self.region.upper() + " summoners empty and no one in flight"

                    self.statisticsLock.release()
                    self.summonersLock.release()

                    if self.rawGamesCollectionActive:
                        print self.region.upper() + " Summoners is empty and some are still in flight or summoner has an invalid value : " + summoner
                        time.sleep(0.5)

        print self.region.upper() + " Stop feeding the raw games collection threads"

        # Barrier
        for lock in threadsLocks:
            lock.acquire()

        print self.region.upper() + " Ending raw games collection"

        self.rawGamesDone = True




        return ;



    def games_filtering(self):

        while self.gamesFilteringActive:

            try:

                summonerId, data = self.rawGames.get(timeout = 0.5)

                d = 0
                cleanGames = []

                if "games" in data:
                    # check if already recorded
                    for game in data["games"]:

                        toRecord = "gameId" in game and not self.recordedGameIds.find_insert(game["gameId"])

                        if (toRecord):

                            # new game
                            # check RANKED
                            if (game["subType"] == "RANKED_SOLO_5x5"):
                                # add it
                                cleanGames.append(game)

                        else:
                            d += 1
                else:
                    # Summoner with no (new or valid) games
                    self.statisticsLock.acquire()
                    self.playersInflight -= 1
                    self.statisticsLock.release()

                if cleanGames:

                    #May wait until a spot is available
                    self.filteredGames.put((summonerId, cleanGames, d))

                else:
                    # Summoner with no (new or valid) games
                    self.statisticsLock.acquire()
                    self.playersInflight -= 1
                    self.statisticsLock.release()



            except Queue.Empty:

                if self.rawGamesDone:
                    self.gamesFilteringActive = False



        print self.region.upper() + " Ending games filtering"

        return;





    def record_games(self):

        while self.recordGamesActive:

            try:
                filteredGame = self.filteredGames.get(timeout = 1)

                gamesToRecord = [filteredGame]

                for i in range(0, self.filteredGames.qsize()):
                    gamesToRecord.append(self.filteredGames.get())

                #logging
                n = 0
                for summonerId, games, d in gamesToRecord:
                    n += games.__len__()

                print self.region.upper() + " Batch of " + str(gamesToRecord.__len__()) + " summoners for a total of " + str(n) + " games"


                playersId = []


                for summonerId, games, d in gamesToRecord:

                    for game in games :

                        #list all necessary players
                        for player in game["fellowPlayers"]:
                            playersId.append(str(player["summonerId"]))
                        playersId.append(summonerId)

                playersId= list(set(playersId))

                stats = self.get_players_rank(playersId)


                for summonerId, games, d in gamesToRecord:

                    #Probably does not impact anything, avoid an explosion of the number of threads which is unlikely
                    # self.flyingGamesSemaphore.acquire()
                    self.statisticsLock.acquire()
                    self.playersInflight -= 1
                    self.statisticsLock.release()
                    thread.start_new_thread(self.compute_game_records, (games, summonerId, d, stats))

            except Queue.Empty:
                if not self.gamesFilteringActive:
                    self.recordGamesActive = False
                else:
                    print self.region.upper() + " Issue with record games, filtered games is empty"

        print self.region.upper() + " Ending games recording"

        return ;



    def compute_game_records(self, games, summonerId, d, stats):


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

        self.recordsLock.acquire()
        self.records.extend(localRecords)
        self.recordsLock.release()

        self.statisticsLock.acquire()
        self.success += s
        self.invalid += i
        self.duplicate += d
        self.playersDone += 1
        self.statisticsLock.release()

        # self.flyingGamesSemaphore.release()


        return ;

    def get_players_rank(self, summonerIds):
        #split in block of 10, fan out to multiple thread
        #Rearrange all of it

        locks = []
        stats = []

        n = summonerIds.__len__()/10 + 1
        if summonerIds.__len__() % 10 == 0:
            n -= 1

        print self.region.upper() + " Starting " + str(n) + " threads to get " + str(summonerIds.__len__()) + " players' rank"

        for i in range(0, n):
            newLock = thread.allocate_lock()
            locks.append(newLock)
            newLock.acquire()
            newStatsChunck = {}
            stats.append(newStatsChunck)

            if i == (n - 1):
                thread.start_new_thread(self.bulk_rank_stats, (summonerIds[10*i:summonerIds.__len__()], newLock, newStatsChunck))
            else:
                thread.start_new_thread(self.bulk_rank_stats, (summonerIds[10*i:10*(i+1)], newLock, newStatsChunck))


        for lock in locks:
            lock.acquire()

        fullStats = {}
        for s in stats:
            fullStats.update(s)

        return fullStats




    def bulk_rank_stats(self, summonerIds, lock, stats):

        # initialize ranks
        for id in summonerIds:
            stats[id] = -1

        requestUrl = self.rankUrl + ','.join(map(str, summonerIds)) + "/entry"

        data = self.api_call(requestUrl)

        if(data is not None):
            for id in summonerIds:

                if id in data.keys() :
                    for entry in data[id] :
                        if(entry["queue"] == "RANKED_SOLO_5x5"):
                            for x in entry["entries"]:
                                stats[id] = rank_conversion(entry["tier"], x["division"])


        else:
            print self.region.upper() + " Rank failure"


        lock.release()

        playersToAppend = []

        if self.targeted:
            # If targeted take all
            for p in summonerIds:
                if stats[p] >= self.minRankLimit and stats[p] <= self.maxRankLimit:
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
            if not self.summonersDone.find_insert(summonerId, insert= False):
                newSummoners.append(summonerId)

        if newSummoners:
            self.summonersLock.acquire()
            self.summoners.extend(newSummoners)
            self.random_discard()
            self.summonersLock.release()

        return ;

    def get_summoner_matchs(self, lock, i):

        while self.rawGamesCollectionActive or not self.summonersToBeTreated.empty():


            try:
                summonerId = self.summonersToBeTreated.get(timeout = 0.5)
                requestUrl = self.matchUrl + summonerId + "/recent"
                apiData = self.api_call(requestUrl)

                if apiData is not None:
                    #Transfer knowledge
                    self.rawGames.put((summonerId,apiData))

                else:
                    self.statisticsLock.acquire()
                    self.playersInflight -= 1
                    self.statisticsLock.release()

            except Queue.Empty :
                pass

        lock.release()

        # print "Thread " + str(i) + " died"

        return ;


    def api_call(self, url, tries = 0) :

        self.apiLock.acquire()


        if(self.apiCalls == MAX_API_CALLS):
            #Reach max on this key go to next key
            self.currentKey = (self.currentKey + 1) % self.apiKey.__len__()

            self.apiCalls = 0

        if(self.apiCalls == 0) :
            #Check if need to wait
            sleepTime = self.firstCallTime[self.currentKey]- time.time() + WAIT_TIME_SECONDS
            if(sleepTime > 0):
                #Need to sleep
                self.totalSleepTime += sleepTime
                time.sleep(sleepTime)

            self.firstCallTime[self.currentKey] = time.time()

        self.apiCalls += 1
        self.totalApiCalls += 1

        self.apiLock.release()

        response = requests.get(url + self.apiKey[self.currentKey])


        if(not response.ok) :
            self.apiLock.acquire()
            print "Error " + str(response.status_code) + " on " + url \
                  # + apiKey[currentKey]
            self.apiErrors += 1
            if(response.status_code == 500):
                self.apiError500 += 1
                if(tries < 3):
                    self.apiLock.release()
                    return self.api_call(url, tries + 1)
            if(response.status_code == 429):
                self.apiError429 += 1
                time.sleep(0.2)
                self.apiLock.release()
                return self.api_call(url, tries)
            self.apiLock.release()
            return None;

        return json.loads(response.content)

    def load_keys(self):

        f = open('keys.txt', 'r')
        for line in f:
            self.apiKey.append("?api_key=" + line[:42])
            self.firstCallTime.append(0)

        f.close()

        return ;


    def load_summoners(self):

        self.summonersLock.acquire()

        f = open('summoners-' + self.region + '.txt', 'r')
        for line in f:
            if line is not "":
                if line[-1:] == "\n":
                    self.summoners.append(line[:-1])
                else:
                    self.summoners.append(line)

        f.close()

        self.summonersLock.release()





    def random_discard(self):

        #remove duplicates
        self.summoners = list(set(self.summoners))

        toDiscard = self.summoners.__len__() - MAX_SUMMONERS

        if toDiscard > 0:

            for i in range(toDiscard):
                toRemove = random.randint(0, self.summoners.__len__() - 1)
                self.summoners.remove(self.summoners[toRemove])

            random.shuffle(self.summoners)




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
    return -50