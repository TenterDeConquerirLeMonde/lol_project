import sys
import thread

import region_collection as region
import read_db as rdb


RUN_TIME = 60


LOG = False

matchUrl = "https://na.api.pvp.net/api/lol/na/v1.3/game/by-summoner/"
rankUrl = "https://na.api.pvp.net/api/lol/na/v2.5/league/by-summoner/"

testSummonerId = "3675"




def run(runTime = RUN_TIME, minRank = 0, maxRank = 36):

    # naLock = thread.allocate_lock()
    # euwLock = thread.allocate_lock()
    #
    # try:

    if runTime >= 3600 :
        region.MAX_SUMMONERS = 100000
        region.SUMMONER_BATCH_SIZE = 100

    na = region.RegionCollection("na", runTime, minRank, maxRank)
    euw = region.RegionCollection("euw", runTime, minRank, maxRank)
    na.gameIdCutoff = 2500000000
    euw.gameIdCutoff = 3180000000

    na.run()
    euw.run()

    na.wait_for_end()
    euw.wait_for_end()

    if runTime > 3600:
        print rdb.average_rank()

    # except KeyboardInterrupt:
    #     print "Trying to stop it now"
    #     na.stopNow(naLock)
    #     euw.stopNow(euwLock)
    #     # na.wait_for_end()
    #     # euw.wait_for_end()
    #
    # naLock.acquire()
    # euwLock.acquire()

def test():
    na = region.RegionCollection("na", RUN_TIME, 0, 36)
    euw = region.RegionCollection("euw", RUN_TIME, 0, 36)
    na.load_keys()
    euw.load_keys()
    na.load_summoners()
    euw.load_challengers()

    print "NA : " + str(na.summoners)
    print "EUW : " + str(euw.summoners)



if __name__ == "__main__":
    # print sys.argv
    if len(sys.argv) == 2:
        if sys.argv[1] == "test" :
            test()
        else:
            run(runTime=int(sys.argv[1]))

    elif len(sys.argv) == 4:
        run(runTime= int(sys.argv[1]), minRank= int(sys.argv[2]), maxRank= int(sys.argv[3]))

    else :
        print "problem with arguments"
