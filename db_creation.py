import sqlite3
conn = sqlite3.connect('lol.db')

c = conn.cursor()

# Create table
c.execute('''CREATE TABLE matchs 
             (gameId integer, 
        champ11 integer, champ12 integer, champ13 integer, champ14 integer, champ15 integer, 
		champ21 integer, champ22 integer, champ23 integer, champ24 integer, champ25 integer,
		player11 integer, player12 integer, player13 integer, player14 integer, player15 integer,
		player21 integer, player22 integer, player23 integer, player24 integer, player25 integer,
		winnerTeam integer)''')

c.execute('''CREATE TABLE players (summonerId integer, rank integer)''')

 #Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()
