import sqlite3
import math
import csv
import print_functions as pf


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

def average_rank(precision):


    MAX_RANK = 30 + precision
    MIN_RANK = 1

    n = int((MAX_RANK - MIN_RANK)/precision)

    conn = sqlite3.connect('lol.db')
    c = conn.cursor()

    matchs = c.execute("SELECT * FROM matchs")
    total = matchs


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

    c.execute("SELECT COUNT(*) FROM matchs")
    totaldb = c.fetchone()[0]

    conn.close()

    print pf.big_statement("Games by average rank")

    for i in range(0, n):

        print(str(MIN_RANK + i*precision) + " <= x < " + str(MIN_RANK + (i+1)*precision) + " : " + str(stats[i]) \
              + " (" + str(format(float(stats[i]*100)/totaldb,'.3f')) + " %)")

    return ;


average_rank(0.5)


