# Import the EvaDB package
import evadb

import mysql.connector
import tweepy
import datetime

if __name__ == "__main__":
    print("Please enter your twitter API consumer key:")
    # twitter API credentials
    consumer_key = input()
    print("Please enter your twitter API consumer secret:")
    consumer_secret = input()

    # access API
    try:
        auth = tweepy.OAuth2AppHandler(consumer_key, consumer_secret)
        api = tweepy.API(auth)
    except Exception as e:
        print(e)
        print("These Twitter API credentials don't seem to work...")

    # create mysql database using connector
    print("Please enter the MySQL host you would like to connect to:")
    host = input()
    print("Please enter the MySQL user you would like to connect:")
    user = input()
    print("Please enter your desired port number:")
    port = input()
    print("Please enter the password for this host and user:")
    password = input()
    print("Please enter a database name:")
    database = input()


    # mysql connector probably not necessary, but i will keep this code here in case i need it later
    '''
    mysqldb = mysql.connector.connect(
        host=host, user=user,password=password,database=database
    )
    mysqlcursor = mysqldb.cursor()
    mysqlcursor.execute("CREATE DATABASE IF NOT EXISTS eva_twitter_test")
    mysqlcursor.execute("CREATE TABLE IF NOT EXISTS tweets (id INTEGER UNIQUE, name VARCHAR(50), screenname VARCHAR(15), text VARCHAR(280), timestamp DATETIME, rtwts INT, likes INT);")

    # search for tweets and commit to mysql db
    for tweet in tweets:
        mysqlcursor.execute("INSERT INTO tweets (id, name, screenname, text, timestamp, rtwts, likes) VALUES ("
                            + tweet.id + ", "+tweet.user.name+ ", "+tweet.user.username+", "+tweet.text+", "
                            + tweet.created_at + ", " + tweet.retweet_count + ", " + tweet.favorite_count +
                            ")")
    mysqldb.commit()
    '''
    # take tweet search query
    print("What search query would you like to search twitter for?")
    search_query = input()
    try:
        tweets = api.search_tweets(search_query)
    except Exception as e:
        print(e)
        print("Twitter search query failed. Were your API credentials valid?")

    # Connect to EvaDB and get a database cursor for running queries
    evacursor = evadb.connect().cursor()

    # create EvaDB database
    try:
        print(evacursor.query("CREATE DATABASE IF NOT EXISTS eva_twitter_test WITH ENGINE = 'mysql', PARAMETERS = {\"user\":\""
                              + user + "\", \"password\":\"" + password + "\", \"host\":\"" + host + "\", \"port\":\"" + port
                              + "\", \"database\":\"" + database + "\"};").df())
    except Exception as e:
        print(e)
        print("Failed to create EvaDB database  - did you input the correct MySQL credentials?")
    # timestamp as text because evadb seems not to support datetime format
    # YYYY-MM-DD_HH:MM:SS is exactly 19 characters
    print(evacursor.query("CREATE TABLE IF NOT EXISTS tweets (id INTEGER UNIQUE, name TEXT(50), screenname TEXT(15), text TEXT(280), timestamp TEXT(19), rtwts INTEGER, likes INTEGER)").df())
    try:
        for tweet in tweets:
            print(evacursor.query("INSERT INTO tweets (id, name, screenname, text, timestamp, rtwts, likes) VALUES ("
                                  + tweet.id + ", \"" + tweet.user.name + "\", \"" + tweet.user.username + "\", \""
                                  + tweet.text + "\", \"" + tweet.created_at.strftime("%Y-%m-%d %H:%M:%S") + "\", "
                                  + tweet.retweet_count + ", " + tweet.favorite_count + ")").df())
    except Exception as e:
        print(e)
        print("Insertion failed - either no list of tweets to insert or database does not exist. Did your search query or databse creation fail?")

    # dead testing code - but it would work
    '''
    print(evacursor.query("INSERT INTO tweets (id, name, screenname, text, timestamp, rtwts, likes) VALUES ("
                          + "1" + ", \"" + "test_name" + "\", \"" + "test_username" + "\", \""
                          + "placeholder_text" + "\", \"" + datetime.datetime(2023,10,17,15,30,0).strftime("%Y-%m-%d %H:%M:%S") + "\", "
                          + "1" + ", " + "1" + ")").df())
    print(evacursor.query("SELECT * from tweets").df())
    '''

    # code syntax is correct but sentiment analysis currently will not work
    # will need to learn postgres and switch from MySQL
    '''
    try:
        print("Performing sentiment analysis...")
        print(evacursor.query("SELECT ChatGPT(\"Is the following tweet positive or negative? Only respond using \'positive\'"
                              + "or \'negative\'. For example, Today is a nice day: positive. I hate Mondays: negative\","
                                "text) FROM " + database + ".tweets;").df())
    except Exception as e:
        print(e)
        print("Sentiment analysis failed")
    '''
