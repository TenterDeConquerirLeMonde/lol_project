import sys

import region_collection as region


RUN_TIME = 60


LOG = False

matchUrl = "https://na.api.pvp.net/api/lol/na/v1.3/game/by-summoner/"
rankUrl = "https://na.api.pvp.net/api/lol/na/v2.5/league/by-summoner/"

testSummonerId = "3675"




def run(runTime = RUN_TIME, minRank = 0, maxRank = 36):

    if runTime > 3600 :
        region.MAX_SUMMONERS = 5000
        region.SUMMONER_BATCH_SIZE = 80

    na = region.RegionCollection("na", runTime, minRank, maxRank)
    euw = region.RegionCollection("euw", runTime, minRank, maxRank)

    na.run()
    euw.run()

    na.wait_for_end()
    euw.wait_for_end()



if __name__ == "__main__":
    # print sys.argv
    if len(sys.argv) == 2:
        run(runTime=int(sys.argv[1]))

    elif len(sys.argv) == 4:
        run(runTime= int(sys.argv[1]), minRank= int(sys.argv[2]), maxRank= int(sys.argv[3]))

    else :
        print "problem with arguments"
