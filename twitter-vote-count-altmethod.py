from pyspark import SparkContext
from pyspark.sql import SQLContext

import pyspark.sql.functions as sqlfunc

from itertools import groupby
from operator import itemgetter
import os, errno

# initialisation
sc = SparkContext(appName="Count Twitter Votes Per Party Over Time Period")
sc.setLogLevel('ERROR')

sqlc = SQLContext(sc)

# real dataframe, two weeks before election
dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/03/{0[0-9],1[0-4]}/*/*")

# real dataframe, poll 4 period
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/02/{1[5-9],2[0-8]}/*/*")

# real dataframe, poll 3 period
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/02/{0[0-9],1[0-4]}/*/*")

# real dataframe, poll 2 period
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/01/{1[5-9],2[0-8]}/*/*")

# real dataframe, poll 1 period
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/01/{0[0-9],1[0-4]}/*/*")

# test dataframe
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/01/01/*/*")

#
# Dictionary mapping the mentioned handles to the party they belong to.
#
HANDLES = {
    'vvd': 'vvd',
    'markrutte': 'vvd',
    'dijkhoff': 'vvd',
    'geertwilderspvv': 'pvv',
    'cdavandaag': 'cda',
    'sybrandbuma': 'cda',
    'd66': 'd66',
    'apechtold': 'd66',
    'groenlinks': 'gl',
    'jesseklaver': 'gl',
    'spnl': 'sp',
    'emileroemer': 'sp',
    'marijnissenl': 'sp',
    'pvda': 'pvda',
    'lodewijka': 'pvda',
    'christenunie': 'cu',
    'gertjansegers': 'cu',
    'partijvddieren': 'pvdd',
    'mariannethieme': 'pvdd',
    '50pluspartij': '50plus',
    'henkkrol': '50plus',
    'sgpnieuws': 'sgp',
    'keesvdstaaij': 'sgp',
    'denknl': 'denk',
    'tunahankuzu': 'denk',
    'fvdemocratie': 'fvd',
    'thierrybaudet': 'fvd',
}

#
# List of Twitter handles (without the @) that we exclude. Includes parties own handles and major news outlets.
#
EXCLUDE_HANDLES = ['VVD', 'markrutte', 'dijkhoff', 'geertwilderspvv', 'cdavandaag', 'sybrandbuma', 'd66',
                   'APechtold', 'groenlinks', 'jesseklaver', 'SPnl', 'emileroemer', 'MarijnissenL', 'PvdA',
                   'LodewijkA', 'christenunie', 'gertjansegers', 'PartijvdDieren', 'mariannethieme',
                   '50pluspartij', 'HenkKrol', 'SGPnieuws', 'keesvdstaaij', 'DenkNL', 'tunahankuzu',
                   'fvdemocratie', 'thierrybaudet', 'NUnl', 'RTLnieuws', 'NPOpolitiek', 'MarioGibbels',
                   'nieuwsuittwente', 'telegraaf', 'NOS', 'ADnl', 'FD_Nieuws', 'financieelblad', 'Metro', 'ndnl', 'nrc',
                   'NRC_commentaar', 'refdag', 'trouw', 'volkskrant', 'BarneveldseKrnt', 'BNDeStem', 'brabantsdagblad',
                   'delimburger', 'dvhn_nl', 'ED_Eindhoven', 'ED_Regio', 'frieschdagblad', 'DeGelderlander',
                   'gooieneemlander', 'hdhaarlem', 'lc_nl', 'leidschdagblad', 'nhdagblad', 'parool', 'pzcredactie',
                   'De_Stentor', 'tubantia', 'foknieuws', 'quotevanvandaag', 'CoronelKart', 'Gemeente', 'Politiek',
                   '2eKamertweets']

#
# method mapping a tweet to a party based on keywords and excluded handles
# see 200-tweets-per-party.py for comments not included here
#
def mapTweetToParty(tweet):
    tweet = tweet.asDict()
    text = tweet['text'].encode('utf8')

    # check if we should exclude handle
    screen_name = tweet['screen_name']
    if screen_name in EXCLUDE_HANDLES:
        return screen_name, [(None, 1)]

    # check if it is a retweet
    rt_screen_name = tweet['rt_screen_name']
    if rt_screen_name is None:
        return screen_name, [(None, 1)]

    # loop over all handles we can identify
    for handle, party in HANDLES.items():
        # check if retweeted handle is equal to the handle from our list
        if handle.lower() == rt_screen_name.lower():
            # if so, this tweet retweets a party
            return screen_name, [(party, 1)]

    return screen_name, [(None, 1)]


#
# below are the helper functions we need for reduce
#

# this is the reduceParty function
# it merges two parties with the same name into one and combine party mention count
def reduceParty(party1, party2):
    return (party1[0], party1[1] + party2[1])


# group party count in each user by party and sort by party count.
#           it transforms (screen_name, [(party1, 1), (party2, 1), (party2, 1)])
#           to (screen_name, [(party2, 2), (party1, 1)])
def groupMap(tuple):
    return (tuple[0], sorted(
        [reduce(reduceParty, parties) for _, parties in
         groupby(sorted(tuple[1], key=itemgetter(0)), key=itemgetter(0))],
        key=itemgetter(1), reverse=True))


# where => filter so that we only have Dutch tweets remaining
# select => select, from a tweet, only the text and the screen name of the tweeter
# map => for every tweet that is now left, determine what party it matches
# filter => filter out all tweets that have 'None' as a party (ie. no match, or multiple matches)
# reduceByKey => combines all tweet-party combination to a tuple (user, [list of parties they tweet about])
# map => see groupMap above
# filter => filter the user that only have 1 party count or doesn't have multiple largest party count
# map => for each user, get the party with largest party count
# reduceByKey => combines all user votes into a total party vote count
tweets = dataFrame.where(dataFrame.lang == 'nl') \
    .select('text', 'user.screen_name', sqlfunc.col('retweeted_status.user.screen_name').alias('rt_screen_name')).rdd \
    .map(mapTweetToParty) \
    .filter(lambda tuple: tuple[1][0][0] is not None) \
    .reduceByKey(lambda a, b: a + b) \
    .map(groupMap) \
    .filter(lambda tuple: len(tuple[1]) == 1 or tuple[1][0][1] != tuple[1][1][1]) \
    .map(lambda tuple: (tuple[1][0][0], 1)) \
    .reduceByKey(lambda a, b: a + b)

# write vote counts to this file
filename = "/home/s1220535/votes_alt_poll1.csv"

# check if folder exists
# from: https://stackoverflow.com/a/12517490
if False and not os.path.exists(os.path.dirname(filename)):
    try:
        os.makedirs(os.path.dirname(filename))
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

# open votes file
f = open(filename, 'w+')

# for each party
for party, vote in tweets.collect():
    # write the number of votes they get to file
    f.writelines("{},{}\r\n".format(party, vote))

# f.close()
