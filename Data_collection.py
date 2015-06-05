__author__ = 'Jeff'


import mysql.connector
import json
import urllib2
import datetime
import time
import threading
from bs4 import BeautifulSoup
from twitter import Twitter, OAuth


# put your tokens, keys, secrets, and Twitter handle in the following variables
CONSUMER_KEY = ""
CONSUMER_SECRET = ""
OAUTH_TOKEN = ""
OAUTH_SECRET = ""

TWITTER_HANDLE = ""


#For twitter
t = Twitter(auth=OAuth(OAUTH_TOKEN, OAUTH_SECRET,
            CONSUMER_KEY, CONSUMER_SECRET))


#collect twitter data
def twitter_score(q):

    tweets = []
    MAX_ATTEMPTS = 10
    COUNT_OF_TWEETS_TO_BE_FETCHED = 1000

    for i in range(0,MAX_ATTEMPTS):
        print i

        if( len(tweets) > COUNT_OF_TWEETS_TO_BE_FETCHED ):
            print "exceed limit!"
            break


        # STEP 1: Query Twitter
        if(0 == i):
            # Query twitter for data.
            results = t.search.tweets( q = q, count = "100", result_type = "recent" )
        else:
            # After the first call we should have max_id from result of previous call. Pass it in query.
            results = t.search.tweets( q = q, count = "100", result_type = "recent", include_entities='true', max_id=next_max_id)


        tweets += results['statuses']


        # STEP 3: Get the next max_id
        try:
            # Parse the data returned to get max_id to be passed in consequent call.
            next_results_url_params = results['search_metadata']['next_results']
            next_max_id = next_results_url_params.split('max_id=')[1].split('&')[0]
        except:
            # No more next pages
            break

    #print tweets

    count_up = 1.00
    count_down = 1.00
    count_love= 1.00
    count_hate = 1.00

    for tweet in tweets:
        count_up += tweet["text"].count("up")
        count_down += tweet["text"].count("down")
        count_love += tweet["text"].count("love")
        count_hate += tweet["text"].count("hate")

    count_list = [ count_up, count_down, count_love, count_hate ]

    return count_list


#collect currency data
def currency():
    try:
        url = "http://webrates.truefx.com/rates/connect.html?f=csv&c=EUR/USD,GBP/USD,USD/JPY"

        response = urllib2.urlopen(url)
        html_doc = response.read()

        soup = BeautifulSoup(html_doc)
        c = soup.get_text().split()
        EURtoUSD = c[0].split(",")
        GBPtoUSD = c[1].split(",")
        USDtoJPY = c[2].split(",")

        EUR_USD = EURtoUSD[2]+EURtoUSD[3]
        GBP_USD = GBPtoUSD[2]+GBPtoUSD[3]
        USD_JPY = USDtoJPY[2]+USDtoJPY[3]


        print""
        print"******************************"
        print""
        print datetime.datetime.now()
        print""
        print "EUR_USD: " + str(EUR_USD)
        print "GBP_USD: " + str(GBP_USD)
        print "USD_JPY: " + str(USD_JPY)
        print""
        print"******************************"
        print""

        currency_list = [ EUR_USD, GBP_USD, USD_JPY ]

        return currency_list

    except Exception,e:
        print("Currency went wrong")
        print str(e)
        pass



#collect blockchain data
def bitcoin_streaming():


    #blockchain data source 
    url1 = "https://blockchain.info/ticker"
    url2 = "https://blockchain.info/stats?format=json"
    next_call = time.time()
    old_price = 1.00



    #database info
    conn = mysql.connector.connect( database="",
                                    host="",
                                    user="",
                                    password="")

    #initialization
    cursor = conn.cursor()
    cursor.execute( "DROP TABLE IF EXISTS price_tweet" )

    cursor.execute( """
      CREATE TABLE price_tweet
      (
        Id INT PRIMARY KEY AUTO_INCREMENT,
        t CHAR(100),
        current_price DOUBLE,
        buy_price DOUBLE,
        sell_price DOUBLE,
        late_price DOUBLE,
        chan DOUBLE,
        change_percent DOUBLE,

        market_price_usd DOUBLE,
        total_fees_btc DOUBLE,
        blocks_size DOUBLE,
        trade_volume_usd DOUBLE,
        estimated_btc_sent DOUBLE,
        timestamp DOUBLE,
        trade_volume_btc DOUBLE,
        totalbc DOUBLE,
        minutes_between_blocks DOUBLE,
        miners_revenue_usd DOUBLE,
        estimated_transaction_volume_usd DOUBLE,
        nextretarget DOUBLE,
        difficulty DOUBLE,
        n_blocks_mined DOUBLE,
        total_btc_sent DOUBLE,
        n_blocks_total DOUBLE,
        miners_revenue_btc DOUBLE,
        hash_rate DOUBLE,
        n_tx DOUBLE,
        n_btc_mined DOUBLE,

        up_tweet DOUBLE,
        down_tweet DOUBLE,
        up_ratio DOUBLE,
        up_difference DOUBLE,
        love_tweet DOUBLE,
        hate_tweet DOUBLE,
        love_ratio DOUBLE,
        love_difference DOUBLE,

        EUR_USD DOUBLE,
        GBP_USD DOUBLE,
        USD_JPY DOUBLE
      )
      """ )


    while True:
        try:
            #data from twitter
            count_list = twitter_score(q)
            up_tweet = float(count_list[0])
            down_tweet = float(count_list[1])
            love_tweet = float(count_list[2])
            hate_tweet = float(count_list[3])

            up_ratio = up_tweet / down_tweet
            up_difference = up_tweet - down_tweet
            love_ratio = love_tweet / hate_tweet
            love_difference = love_tweet - hate_tweet

            #data source
            data1 = json.load( urllib2.urlopen(url1) )
            data2 = json.load( urllib2.urlopen(url2) )

            #price data
            current_price = (data1["USD"]["last"])
            buy_price = (data1["USD"]["buy"])
            sell_price = (data1["USD"]["sell"])
            late_price = (data1["USD"]["15m"])
            chan = current_price - old_price
            change_percent = chan / old_price

            #other bitcoin data
            market_price_usd = (data2["market_price_usd"])
            total_fees_btc = (data2["total_fees_btc"])
            blocks_size = (data2["blocks_size"])
            trade_volume_usd = (data2["trade_volume_usd"])
            estimated_btc_sent = (data2["estimated_btc_sent"])
            timestamp = (data2["timestamp"])
            trade_volume_btc = (data2["trade_volume_btc"])
            totalbc = (data2["totalbc"])
            minutes_between_blocks = (data2["minutes_between_blocks"])
            miners_revenue_usd = (data2["miners_revenue_usd"])
            estimated_transaction_volume_usd = (data2["estimated_transaction_volume_usd"])
            nextretarget = (data2["nextretarget"])
            difficulty = (data2["difficulty"])
            n_blocks_mined = (data2["n_blocks_mined"])
            total_btc_sent = (data2["total_btc_sent"])
            n_blocks_total = (data2["n_blocks_total"])
            miners_revenue_btc = (data2["miners_revenue_btc"])
            hash_rate = (data2["hash_rate"])
            n_tx = (data2["n_tx"])
            n_btc_mined = (data2["n_btc_mined"])

            #currency data
            currency_list = currency()
            EUR_USD = float(currency_list[0])
            GBP_USD = float(currency_list[1])
            USD_JPY = float(currency_list[2])

            #time
            current_t = datetime.datetime.now()

            #print info
            print current_t
            print""
            print "$"+ str(current_price)
            print""
            print "up_tweet: "+ str(up_tweet)
            print "down_tweet: "+ str(down_tweet)
            print "up_ratio: "+ str(up_ratio)
            print""
            print "love_tweet: "+ str(love_tweet)
            print "hate_tweet: "+ str(hate_tweet)
            print "love_ratio: "+ str(love_ratio)
            print""
            print"******************************"
            print""


            #store to mysql
            try:
                add_info = (
                            "INSERT INTO price_tweet "

                            "(t, current_price, buy_price, sell_price, late_price, chan, change_percent, market_price_usd, "
                            "total_fees_btc, blocks_size, trade_volume_usd, estimated_btc_sent, timestamp, trade_volume_btc, "
                            "totalbc, minutes_between_blocks, miners_revenue_usd, estimated_transaction_volume_usd, nextretarget, "
                            "difficulty, n_blocks_mined, total_btc_sent, n_blocks_total, miners_revenue_btc, hash_rate, n_tx, n_btc_mined, "
                            "up_tweet, down_tweet, up_ratio, up_difference, love_tweet, hate_tweet, love_ratio, love_difference, EUR_USD, GBP_USD, USD_JPY) "

                            "VALUES ( %(t)s, %(current_price)s, %(buy_price)s, %(sell_price)s, %(late_price)s, %(chan)s, %(change_percent)s, "
                            "%(market_price_usd)s, %(total_fees_btc)s, %(blocks_size)s, %(trade_volume_usd)s, %(estimated_btc_sent)s, %(timestamp)s, "
                            "%(trade_volume_btc)s, %(totalbc)s, %(minutes_between_blocks)s, %(miners_revenue_usd)s, %(estimated_transaction_volume_usd)s, "
                            "%(nextretarget)s, %(difficulty)s, %(n_blocks_mined)s, %(total_btc_sent)s, %(n_blocks_total)s, %(miners_revenue_btc)s, %(hash_rate)s, "
                            "%(n_tx)s, %(n_btc_mined)s, %(up_tweet)s, %(down_tweet)s, %(up_ratio)s, %(up_difference)s, %(love_tweet)s, %(hate_tweet)s, %(love_ratio)s, %(love_difference)s, "
                            "%(EUR_USD)s, %(GBP_USD)s, %(USD_JPY)s)"
                            )

                data_info = {
                            't': current_t,
                            'current_price': current_price,
                            'buy_price': buy_price,
                            'sell_price': sell_price,
                            'late_price': late_price,
                            'chan': chan,
                            'change_percent': change_percent,

                            "market_price_usd": market_price_usd,
                            "total_fees_btc": total_fees_btc,
                            "blocks_size": blocks_size,
                            "trade_volume_usd": trade_volume_usd,
                            "estimated_btc_sent": estimated_btc_sent,
                            "timestamp": timestamp,
                            "trade_volume_btc": trade_volume_btc,
                            "totalbc": totalbc,
                            "minutes_between_blocks": minutes_between_blocks,
                            "miners_revenue_usd": miners_revenue_usd,
                            "estimated_transaction_volume_usd": estimated_transaction_volume_usd,
                            "nextretarget": nextretarget,
                            "difficulty": difficulty,
                            "n_blocks_mined": n_blocks_mined,
                            "total_btc_sent": total_btc_sent,
                            "n_blocks_total": n_blocks_total,
                            "miners_revenue_btc": miners_revenue_btc,
                            "hash_rate": hash_rate,
                            "n_tx": n_tx,
                            "n_btc_mined": n_btc_mined,

                            "up_tweet": up_tweet,
                            "down_tweet": down_tweet,
                            "up_ratio": up_ratio,
                            "up_difference": up_difference,
                            "love_tweet": love_tweet,
                            "hate_tweet": hate_tweet,
                            "love_ratio": love_ratio,
                            "love_difference": love_difference,

                            'EUR_USD': EUR_USD,
                            'GBP_USD': GBP_USD,
                            'USD_JPY': USD_JPY
                            }

                cursor.execute( add_info, data_info )

                conn.commit()

            except mysql.connector.Error as err:
                print("DB went wrong: {}".format(err))
                pass


            next_call += 60 # <--- frequency
            time.sleep(next_call - time.time())
            old_price = current_price

        except Exception,e:
            print("Something went wrong")
            print str(e)
            pass


    cursor.close ()
    conn.close()



#twitter search key
q = "#bitcoin" # <--- put what you like to search

timerThread = threading.Thread( target=bitcoin_streaming )

timerThread.start()


#END


