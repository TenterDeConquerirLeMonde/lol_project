import sqlite3
conn = sqlite3.connect('lol.db')

c = conn.cursor()

matchs = c.execute("SELECT * FROM matchs")

print("Matchs tables : \n\n")

for row in matchs:
    print row

players = c.execute("SELECT * FROM players")

print("\n\nPlayers tables : \n\n")


for row in players:
    print row


conn.close()