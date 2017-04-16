import sqlite3
import requests
import json
import time
import random
import thread
import operator

import print_functions as pf

MAX_API_CALLS = 10
WAIT_TIME_SECONDS = 12
RUN_TIME = 3600
MAX_SUMMONERS = 200
INTERMEDIATE_REPORT = 15
LOG = False

matchUrl = "https://na.api.pvp.net/api/lol/na/v1.3/game/by-summoner/"
rankUrl = "https://na.api.pvp.net/api/lol/na/v2.5/league/by-summoner/"

testSummonerId = "59781731"

#testApiKey = "?api_key=" + "RGAPI-B34D1399-0E8A-4DF4-86A2-FF6B9264E79A"

apiKey = []
firstCallTime = []
apiCalls = 0
totalApiCalls = 0
totalSleepTime = 0
currentKey = 0
apiError500 = 0
apiError429 = 0
apiErrors = 0

globalLock = thread.allocate_lock()

summoners = [testSummonerId]



def run():

    global summoners

    startTime = time.time()

    conn = sqlite3.connect('lol.db')

    c = conn.cursor()

    load_keys()
    load_summoners()

    success = 0
    invalid = 0
    duplicate = 0

    n = 0

    successList = []
    invalidList = []
    duplicateList = []

    if LOG:
        log = open('log.txt', 'w')


    while summoners and (time.time() - startTime) < RUN_TIME:

        if n % INTERMEDIATE_REPORT == 0 and n > 0:
            print pf.big_statement(str(success) + " games recorded (" + str(duplicate) + " duplicates and " + str(invalid) \
                          + " invalid ones) so far with " + str(n) + " players in " + pf.time_format(time.time() - startTime))

        # print summoners
        if LOG:
            log.write(str(summoners) + "\n")

        summoner = summoners.pop(0)

        # log.write(summoner + "\n")

        requestUrl = matchUrl + summoner + "/recent"
        data = api_call(requestUrl)

        # response = requests.get(requestUrl + testApiKey)
        # print requestUrl + testApiKey
        # print("Matchs request : " + str(response.status_code))

        if(data is not None):

            # print "Recording games from summoner : " + str(summoner)
            n += 1
            s,d,i = record_games(summoner, data["games"], c)

            success += s
            invalid += i
            duplicate += d

            successList.append(s)
            invalidList.append(i)
            duplicateList.append(d)

            random_discard()

        conn.commit()
        #conn.close()


    finalStatement = pf.big_statement(str(success) + " games recorded (" + str(duplicate) + " duplicates and " + str(invalid)\
                  + " invalid games) in " + pf.time_format(time.time() - startTime) \
              + " using " + str(totalApiCalls) + " API calls (" +str(apiErrors) + " errors, 500 : " + str(apiError500) \
            + ", 429 : " + str(apiError429) + ") and sleeping for " + pf.time_format(totalSleepTime))

    print finalStatement

    f = open('summoners.txt', 'r+')
    f.writelines("\n".join(summoners))

    print "storing " + str(summoners.__len__()) + " summonerIds for next time"

    c.execute("SELECT COUNT(*) FROM matchs")
    totaldb = c.fetchone()[0]

    conn.close()
    if LOG:
        log.close()

    print pf.big_statement("The database contains " + str(totaldb) + " games")

    report(totaldb, successList, invalidList, duplicateList, finalStatement)

    return ;


def record_games(summonerId, games, c):
    #Parse the json and add to the DB
    i = 0
    invalid = 0
    duplicate = 0
    global summoners

    locks = []
    records = []

    for game in games:
        # start = time.time()
        #check if already recorded
        sqlCheck = "SELECT champ11 FROM matchs WHERE gameId = " + str(game["gameId"])
        # print sqlCheck
        c.execute(sqlCheck)
        #print test
        previous = c.fetchone()
        if(previous is None):

            #new game
            #check RANKED
            if(game["subType"] == "RANKED_SOLO_5x5"):
                i += 1

                newRecord = []
                newLock = thread.allocate_lock()
                records.append(newRecord)
                locks.append(newLock)

                thread.start_new_thread(compute_game_record,[game, summonerId, newRecord, newLock])

        else:
            # print("Game already recorded")
            duplicate += 1

    #Wait for all thread to finish

    for lock in locks:
        lock.acquire()

    #check for invalid records and commit others to DB

    for record in records:

        if record.__contains__(-1):
            # print "Invalid record"
            i -= 1
            invalid += 1

        else:

            sqlAction = "INSERT INTO matchs VALUES(" + ','.join(map(str, record)) + ")"
            c.execute(sqlAction)

    # print(str(i - 1) + " games added to db")
    print("Summoner " + summonerId + " : " + str(i) + " new games, " + str(duplicate) + " duplicate games and " \
          + str(invalid) + " invalid")
    return i, duplicate, invalid;

def compute_game_record(game, summonerId, record, lock):

    lock.acquire()

    champsTeam1 = []
    champsTeam2 = []
    levelTeam1 = []
    levelTeam2 = []

    players = []
    for player in game["fellowPlayers"]:
        players.append(str(player["summonerId"]))

    # select 2 highest and 2 lowest players
    # calculate the rank
    stats = bulk_rank_stats(players)
    # sort players by rank asc
    stats_cp = sorted(stats.items(), key=operator.itemgetter(1))
    # select the chosen 4 !
    playersToAppened = []
    for k in range(2):
        playersToAppened.append(stats_cp[k][0])
        playersToAppened.append(stats_cp[len(stats_cp) - 1 - k][0])

    summoners.extend(playersToAppened)
    players.append(summonerId)
    # get bulk ranks for all players in the game

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


    lock.release()
    return ;

def bulk_rank_stats(summonerIds):
    #initialize ranks
    stats = dict()
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


    return stats;




def api_call(url, tries = 0) :

    global globalLock
    global apiCalls
    global firstCallTime
    global totalApiCalls
    global totalSleepTime
    global currentKey
    global apiError429
    global apiError500
    global apiErrors

    globalLock.acquire()


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

    globalLock.release()

    response = requests.get(url + apiKey[currentKey])


    if(not response.ok) :
        globalLock.acquire()
        print "Error " + str(response.status_code) + " on " + url + apiKey[currentKey]
        apiErrors += 1
        if(response.status_code == 500):
            apiError500 += 1
            if(tries < 3):
                globalLock.release()
                return api_call(url, tries + 1)
        if(response.status_code == 429):
            apiError429 += 1
        globalLock.release()
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
    f = open('summoners.txt', 'r+')
    for line in f:
        if line is not "":
            summoners.append(line[:-1])

    f.seek(0)
    f.truncate()
    f.close()
    random_discard()


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

    rank = 0
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

    #TODO : deal with Master


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
    #remove doubles
    summoners = list(set(summoners))

    toDiscard = summoners.__len__() - MAX_SUMMONERS

    # print("Discarding " + str(toDiscard) + " players")

    if toDiscard > 0:


        f = open('summoners.txt', 'r+')

        for i in range(toDiscard):
            toRemove = random.randint(0, summoners.__len__() - 1)
            if random.randint(0, 99) < 20 :
                f.write(summoners[toRemove] + "\n")

            summoners.remove(summoners[toRemove])

        random.shuffle(summoners)

    # print summoners

    return;




run()

