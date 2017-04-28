import sqlite3
import math
import sys
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

def average_rank(precision, lowerLimit = 0, higherLimit = 36):


    MAX_RANK = 35 + precision
    MIN_RANK = 1

    n = int((MAX_RANK - MIN_RANK)/precision)

    conn = sqlite3.connect('lol.db')
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

    c.execute("SELECT COUNT(*) FROM matchs")
    totaldb = c.fetchone()[0]

    conn.close()

    output = pf.big_statement("Games by average rank")

    games = 0

    for i in range(0, n):

        if (MIN_RANK + i * precision) >= lowerLimit and (MIN_RANK + (i + 1) * precision) <= higherLimit:

            output += (str(MIN_RANK + i*precision) + " <= x < " + str(MIN_RANK + (i+1)*precision) + " : " + str(stats[i]) \
              + " (" + str(format(float(stats[i]*100)/totaldb,'.3f')) + " %)\n")
            games += stats[i]

    output += "\n\n" + str(games) + " games with average rank between " + str(lowerLimit) + " and " + str(higherLimit)

    return output;

if __name__ == "__main__":
    if len(sys.argv) == 2:
        print average_rank(float(sys.argv[1]))
    if len(sys.argv) == 3:
        print average_rank(float(sys.argv[1]), lowerLimit= int(sys.argv[2]))
    if len(sys.argv) == 4:
        print average_rank(float(sys.argv[1]), lowerLimit= int(sys.argv[2]), higherLimit= int(sys.argv[3]))
    else:
        average_rank(0.5)

