import sqlite3
import requests
import json
import time

MAX_API_CALLS = 10
WAIT_TIME_SECONDS = 12

matchUrl = "https://na.api.pvp.net/api/lol/na/v1.3/game/by-summoner/"
rankUrl = "https://na.api.riotgames.com/api/lol/NA/v2.5/league/by-summoner/"

testSummonerId = "59781731"

testApiKey = "?api_key=" + "RGAPI-B34D1399-0E8A-4DF4-86A2-FF6B9264E79A"

apiKey = []
firstCallTime = []
apiCalls = 0
totalApiCalls = 0
totalSleepTime = 0
currentKey = 0


def run():

	startTime = time.time()

	conn = sqlite3.connect('lol.db')

	c = conn.cursor()

	load_keys()

	requestUrl = matchUrl + testSummonerId + "/recent"
	data = api_call(requestUrl)

	response = requests.get(requestUrl + testApiKey)
	print requestUrl + testApiKey
	print("Matchs request : " + str(response.status_code))

	if(data is not None):

		success = record_games(testSummonerId, data["games"], c)

	conn.commit()
	conn.close()

	big_statement(str(success) + " games recorded in " + str(format((time.time() - startTime), '.2f')) \
		  + " s using " + str(totalApiCalls) + " API calls and sleeping for " + str(format(totalSleepTime, '.2f')) + " s")

	return ;


def record_games(summonerId, games, c):
	#Parse the json and add to the DB
	i = 0

	for game in games:
		start = time.time()
		#check if already recorded
		sqlCheck = "SELECT champ11 FROM matchs WHERE gameId = " + str(game["gameId"])
		# print sqlCheck
		c.execute(sqlCheck)
		#print test
		previous = c.fetchone()
		# TODO : use test
		if(previous is None):
			i += 1
			#new game
			#check RANKED
			if(game["subType"] == "RANKED_SOLO_5x5"):

				champsTeam1 = []
				champsTeam2 = []
				levelTeam1 = []
				levelTeam2 = []

				players = [summonerId]
				for player in game["fellowPlayers"]:
					players.append(str(player["summonerId"]))

				#get bulk ranks for all players in the game

				stats = bulk_rank_stats(players)

				if(game["teamId"] == 100):
					champsTeam1.append(game["championId"])
					levelTeam1.append(stats[summonerId])
				else:
					champsTeam2.append(game["championId"])
					levelTeam2.append(stats[summonerId])

				for player in game["fellowPlayers"] :
					if(player["teamId"] == 100):
						champsTeam1.append(player["championId"])
						levelTeam1.append(stats[str(player["summonerId"])])
					else :
						champsTeam2.append(player["championId"])
						levelTeam2.append(stats[str(player["summonerId"])])


				record = [game["gameId"]]
				record.extend(champsTeam1)
				record.extend(champsTeam2)
				record.extend(levelTeam1)
				record.extend(levelTeam2)

				sqlAction = "INSERT INTO matchs VALUES(" + ','.join(map(str, record)) + ")"
				c.execute(sqlAction)
				print("New Game " + str(i) + " : " + str(record) + " computed in " + str(format((time.time() - start), '.2f')) + "s")


		else:
			print("Game already recorded")

	# print(str(i - 1) + " games added to db")
	return i;



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

			for entry in data[id] :
				if(entry["queue"] == "RANKED_SOLO_5x5"):
					rank = 0
					for x in entry["entries"]:
						stats[id] = rank_conversion(entry["tier"], x["division"])


	else:
		print "Rank failure"


	return stats;

# def rank_stats(summonerId, c):
# 	# check db
# 	rank = -1
# 	c.execute("SELECT rank FROM players WHERE summonerId = " + str(summonerId))
# 	knowRank = c.fetchone()
# 	if (knowRank is not None):
# 		# print ("Summoner rank already know : " + str(knowRank[0]))
# 		return knowRank[0]
#
#
# 	else:
# 		requestUrl = rankUrl + summonerId + "/entry"
# 		# print requestUrl
#
# 		data = api_call(requestUrl)
#
# 		if (data is not None):
# 			#	print("rank response ok")
#
# 			for entry in data[summonerId]:
# 				if (entry["queue"] == "RANKED_SOLO_5x5"):
# 					rank = 0
# 					for x in entry["entries"]:
# 						rank = rank_conversion(entry["tier"], x["division"])
#
# 			c.execute("INSERT INTO players VALUES (" + summonerId + "," + str(rank) + ")")
#
# 		else:
# 			print "Rank failure"
#
# 		return rank;



def api_call(url, tries = 0) :

	global apiCalls
	global firstCallTime
	global totalApiCalls
	global totalSleepTime
	global currentKey

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

	response = requests.get(url + apiKey[currentKey])
	apiCalls += 1
	totalApiCalls += 1

	if(not response.ok) :
		print "Error " + str(response.status_code) + " on " + url + apiKey[currentKey]
		if(response.status_code == 500):
			if(tries < 3):
				return apiCalls(url, tries + 1)
		return None;

	return json.loads(response.content)

def load_keys():

	global firstCallTime
	global apiKey

	f = open('keys.txt', 'r')
	for line in f:
		apiKey.append("?api_key=" + line[:-2])
		firstCallTime.append(0)

	f.close()
	for key in apiKey:
		print key

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

def big_statement(statement):
	dot = 100
	star = 7
	space = 5
	print  "\n\n" + dot*"-" +"\n" + star*"*" + space*" " + statement + space*" " + star*"*" + "\n" + dot*"-" + "\n\n"
	return ;


run()

