# Import the EvaDB package
import evadb

import mysql.connector
import tweepy

# twitter API credentials
consumer_key = ''
consumer_secret = ''


# access API
auth = tweepy.OAuth2AppHandler(consumer_key, consumer_secret)
api = tweepy.API(auth)

# create mysql database using connector
mysqldb = mysql.connector.connect(
    host="localhost", user="root",password="password",database="eva_twitter_test"
)
mysqlcursor = mysqldb.cursor()
mysqlcursor.execute("CREATE DATABASE IF NOT EXISTS eva_twitter_test")
mysqlcursor.execute("CREATE TABLE IF NOT EXISTS tweets (id INTEGER UNIQUE, name VARCHAR(50), screenname VARCHAR(15), text VARCHAR(280), timestamp DATETIME, rtwts INT, likes INT);")

# search for tweets and commit to mysql db
search_query = ''
tweets = api.search_tweets(search_query)

for tweet in tweets:
    mysqlcursor.execute("INSERT INTO tweets (id, name, screenname, text, timestamp, rtwts, likes) VALUES ("
                        + tweet.id + ", "+tweet.user.name+ ", "+tweet.user.username+", "+tweet.text+", "
                        + tweet.created_at + ", " + tweet.retweet_count + ", " + tweet.favorite_count +
                        ")")
mysqldb.commit()

# Connect to EvaDB and get a database cursor for running queries
cursor = evadb.connect().cursor()

# create EvaDB database based on mysql database
print(cursor.query("CREATE DATABASE IF NOT EXISTS eva_twitter_test WITH ENGINE = 'mysql', PARAMETERS = {\"user\":\"root\","
             + "\"password\": \"password\",\"host\":\"localhost\",\"port\":\"3306\",\"database\": \"eva_twitter_test\"};").df())
print(cursor.query("CREATE TABLE IF NOT EXISTS tweets (id INTEGER UNIQUE, name TEXT(50), screenname TEXT(15), text TEXT(280), timestamp ANYTYPE, rtwts INTEGER, likes INTEGER)"))