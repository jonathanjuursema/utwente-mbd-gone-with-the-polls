from pyspark import SparkContext
from pyspark.sql import SQLContext
from itertools import groupby
from operator import itemgetter
import os, errno

# initialisation
sc = SparkContext(appName="Count Twitter Votes Per Party Over Time Period")
sc.setLogLevel('ERROR')

sqlc = SQLContext(sc)

# real dataframe, two weeks before election
dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/03/{0[0-9],1[0-5]}/*/*")

# real dataframe, poll 2 period
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/02/{1[5-9],2[0-8]}/*/*")

# test dataframe for running on farm01
# dataFrame = sqlc.read.json("/data/doina/Twitter-Archive.org/01/01/*/*")

KEYWORDS = {
    "Volkspartij voor Vrijheid en Democratie": 'vvd',
    "VVD": 'vvd',
    "Mark Rutte": 'vvd',
    "Rutte": 'vvd',
    "Klaas Dijkhoff": 'vvd',
    "markrutte": 'vvd',
    "dijkhoff": 'vvd',
    "Partij voor de Vrijheid": 'pvv',
    "PVV": 'pvv',
    "Geert Wilders": 'pvv',
    "Wilders": 'pvv',
    "geertwilderspvv": 'pvv',
    "Christen-Democratish Appel": 'cda',
    "CDA": 'cda',
    "Sybrand van Haersma Buma": 'cda',
    "Sybrand Buma": 'cda',
    "Buma": 'cda',
    "cdavandaag": 'cda',
    "sybrandbuma": 'cda',
    "Democraten 66": 'd66',
    "D66": 'd66',
    "Alexander Pechtold": 'd66',
    "Pechtold": 'd66',
    "d66": 'd66',
    "APechtold": 'd66',
    "GroenLinks": 'gl',
    "GL": 'gl',
    "Jesse Klaver": 'gl',
    "Klaver": 'gl',
    "groenlinks": 'gl',
    "jesseklaver": 'gl',
    "Socialistische Partij": 'sp',
    "SP": 'sp',
    "Emile Roemer": 'sp',
    "Roemer": 'sp',
    "Lilian Marijnissen": 'sp',
    "SPnl": 'sp',
    "emileroemer": 'sp',
    "MarijnissenL": 'sp',
    "Partij van de Arbeid": 'pvda',
    "PvdA": 'pvda',
    "Lodewijk Asscher": 'pvda',
    "Asscher": 'pvda',
    "LodewijkA": 'pvda',
    "ChristenUnie": 'cu',
    "CU": 'cu',
    "Gert-Jan Segers": 'cu',
    "Segers": 'cu',
    "christenunie": 'cu',
    "gertjansegers": 'cu',
    "Partij voor de Dieren": 'pvdd',
    "PvdD": 'pvdd',
    "Marianne Thieme": 'pvdd',
    "Thieme": 'pvdd',
    "PartijvdDieren": 'pvdd',
    "mariannethieme": 'pvdd',
    "50Plus": '50p',
    "Henk Krol": '50p',
    "Krol": '50p',
    "50pluspartij": '50p',
    "HenkKrol": '50p',
    "Staatkundig Gereformeerde Partij": 'sgp',
    "SGP": 'sgp',
    "Kees van der Staaij": 'sgp',
    "Staaij": 'sgp',
    "SGPnieuws": 'sgp',
    "keesvdstaaij": 'sgp',
    "Denk": 'denk',
    "Tunahan Kuzu": 'denk',
    "Kuzu": 'denk',
    "DenkNL": 'denk',
    "tunahankuzu": 'denk',
    "Forum voor Democratie": 'fvd',
    "FvD": 'fvd',
    "Thierry Baudet": 'fvd',
    "Baudet": 'fvd',
    "fvdemocratie": 'fvd',
    "thierrybaudet": 'fvd',
}

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
#


def mapTweetToParty(tweet):
    tweet = tweet.asDict()
    text = tweet['text'].encode('utf8')

    # check if we should exclude handle
    screen_name = tweet['screen_name']
    if screen_name in EXCLUDE_HANDLES:
        return (screen_name, [(None, 1)])

    # extract party
    words = text.replace('@', '').replace('#', '').lower().split()
    parties = set()
    for keyword, party in KEYWORDS.items():
        if keyword.lower() in words:
            parties.add(party)

    if len(parties) != 1:
        return (screen_name, [(None, 1)])
    else:
        return (screen_name, [(parties.pop(), 1)])


#
# below are the helper functions we need for reduce
#

# this is the reduceParty function
# it merges two parties with the same name into one and combine party mention count
def reduceParty(party1, party2):
    return (party1[0], party1[1] + party2[1])


tweets = dataFrame.where(dataFrame.lang == 'nl').select('text', 'user.screen_name').rdd \
    .map(mapTweetToParty) \
    .filter(lambda tuple: tuple[1][0][0] is not None) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda tuple: (tuple[0], sorted([reduce(reduceParty, parties) for _, parties in groupby(sorted(tuple[1], key=itemgetter(0)), key=itemgetter(0))], key=itemgetter(1), reverse=True))) \
    .filter(lambda tuple: len(tuple[1]) == 1 or tuple[1][0][1] != tuple[1][1][1]) \
    .map(lambda tuple: (tuple[1][0][0], 1)) \
    .reduceByKey(lambda a, b: a + b)

filename = "/home/s1895508/votes.csv"

# check if folder exists
# from: https://stackoverflow.com/a/12517490
if not os.path.exists(os.path.dirname(filename)):
    try:
        os.makedirs(os.path.dirname(filename))
    except OSError as exc:  # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

# write file for party
f = open(filename, 'w+')

for party, vote in tweets.collect():
    f.writelines("{},{}".format(party, vote))

f.close()
