import sqlite3
import math
import sys
import time
import print_functions as pf

MAX_RANK = 35
MIN_RANK = 1
def display_db():

    conn = sqlite3.connect('lol.db')

    c = conn.cursor()

    matchs = c.execute("SELECT * FROM matchs LIMIT 20")


    print("Matchs tables : \n\n")

    for row in matchs:
        print row

    c.execute("SELECT COUNT(*) FROM matchs")
    totaldb = c.fetchone()[0]

    print ("\nTotal of " + str(totaldb) + " matchs recorded")

    # players = c.execute("SELECT * FROM players ")
    #
    # print("\n\nPlayers tables : \n\n")
    #
    #
    # for row in players:
    #     print row


    conn.close()

    return ;

def average_rank_region(region, precision):

    n = int((MAX_RANK + precision - MIN_RANK)/precision)

    conn = sqlite3.connect('lol-' + region + '.db')
    c = conn.cursor()

    matchs = c.execute("SELECT * FROM matchs")

    #initialize stats with the desired granularity

    stats = []

    for i in range(0, n):
        stats.append(0)


    for row in matchs:
        values = list(row)
        x = 0
        for i in range(11,21):
            x += int(values[i])

        x /= 10.0
        # print x
        # print int(math.floor(x / precision))
        stats[int(math.floor(x / precision - MIN_RANK/precision)) ] += 1

    conn.close()

    return stats

def average_rank(region = "", precision = 1.0, lowerLimit = 0, higherLimit = 36):

    # stats = []


    if region == "":
        stats = average_rank_region("na", precision)
        statsEuw = average_rank_region("euw", precision)

        for i in range(0, stats.__len__()):
            stats[i] += statsEuw[i]
    else:
        stats = average_rank_region(region, precision)

    intro = "Games by average rank"

    if region is not "":
        intro += " for " + region.upper()

    output = pf.big_statement(intro)

    games = 0
    totaldb = sum(stats)

    for i in range(0, stats.__len__()):

        if (MIN_RANK + i * precision) >= lowerLimit and (MIN_RANK + (i + 1) * precision) <= higherLimit:

            output += (str(MIN_RANK + i*precision) + " <= x < " + str(MIN_RANK + (i+1)*precision) + " : " + str(stats[i]) \
              + " (" + str(format(float(stats[i]*100)/totaldb,'.3f')) + " %)\n")
            games += stats[i]

    output += "\n\n" + str(games) + " games (" + str(format(float(games)*100/totaldb, '.2f')) + "%) with average rank between " + str(lowerLimit) + " and " + str(higherLimit)

    if precision == 1.0 :

        statsFile = open('games_stats.txt', 'w')
        statsFile.writelines(map(str, stats))
        statsFile.close()


    return output;

if __name__ == "__main__":
    if len(sys.argv) == 2:
        print average_rank(precision= float(sys.argv[1]))
    if len(sys.argv) == 3:
        print average_rank(region=sys.argv[1], precision= float(sys.argv[2]))
    if len(sys.argv) == 4:
        print average_rank(precision=float(sys.argv[1]), lowerLimit= int(sys.argv[2]), higherLimit= int(sys.argv[3]))
    if len(sys.argv) == 5:
        print average_rank(sys.argv[1], precision= float(sys.argv[2]), lowerLimit= int(sys.argv[3]), higherLimit= int(sys.argv[4]))
    else:
        print average_rank("", precision=1)

